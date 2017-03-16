/* 
 * File:   conversions.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#ifndef CACHE_H
#define CACHE_H

#include <iostream> 

#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <sys/mman.h>
#include <map>
#include <vector>

#include "../../util/logger.h"
#include "../vertexlock-manager.h"
//缓存 session ID
#define CACHED_SESSION_ID (-1)


namespace dsm {
    
    static size_t get_filesize(std::string filename);
    

    
    struct cached_block {
        size_t len;
        void * data;
        
        cached_block(size_t len, void * data) : len(len), data(data) {}
        
        ~cached_block() {
            free(data);
            data = NULL;
        }
    };
    
    
    /**
      * Simple cache attached to the io manager. 
      */
    class block_cache {
        size_t cache_budget_bytes;
        size_t cache_size;
        cachelock lock;  // TODO: read-write-lock
        bool full;
        std::map<std::string, cached_block *> cachemap;
        
        size_t hits, misses;
        
    public:
    
        block_cache(size_t cache_budget_bytes) : cache_budget_bytes(cache_budget_bytes), cache_size(0), full(false) {
            hits = misses = 0;
        }
        
        ~block_cache() {
            if (hits + misses > 0) {
                logstream(LOG_INFO) << "Cache stats: hits=" << hits << " misses=" << misses << std::endl;
                logstream(LOG_INFO) << " -- in total had " << (cache_size / 1024 / 1024) << " MB in cache." << std::endl;
            }
            std::map<std::string, cached_block *>::iterator it = cachemap.begin();
            for(; it != cachemap.end(); ++it) {
                delete it->second;
            }
        }
        
         
        
        bool consider_caching(std::string filename, void * data, size_t len) {
            bool did_cache = false;
            if (!full && len + cache_size <= cache_budget_bytes) {
                lock.lock();
                if (len + cache_size <= cache_budget_bytes) {
                    cache_size += len;
                    did_cache = true;
                    if (cachemap.size() % 40 == 0) {
                        logstream(LOG_DEBUG) << "Cache size: " << cache_size << " / " << cache_budget_bytes << std::endl;
                    }
                    cachemap.insert(std::pair<std::string, cached_block*>(filename, new cached_block(len, data)));
                }
                if (cache_size > cache_budget_bytes) {
                    full = true; // If full, we can avoid locking如果cache满了,就能避免加锁-
                }
                lock.unlock();
            }
            return did_cache;
        }
        
        void * get_cached(std::string filename) {
            bool acquired_mutex = false;
            if (!full) {
                acquired_mutex = true;
                lock.lock();
            }
            
            void * ret = NULL;
            std::map<std::string, cached_block *>::iterator lookup = cachemap.find(filename);
            if (lookup != cachemap.end()) {
                ret =  lookup->second->data;
                hits++;
            } else {
                misses++;
            }
            
            if (acquired_mutex) {
                lock.unlock();
            }
            return ret;
        }
     
    };
    
    
    
  
   
    static size_t get_filesize(std::string filename) {
        std::string fname = filename;
        int f = open(fname.c_str(), O_RDONLY);
        
        if (f < 0) {
            logstream(LOG_ERROR) << "Could not open file " << filename << " error: " << strerror(errno) << std::endl;
            assert(false);
        }
        //lseek寻找文件尾
        off_t sz = lseek(f, 0, SEEK_END);
        close(f);
        return sz;
    }
    
    
    
}


#endif


