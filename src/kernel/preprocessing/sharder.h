/* 
 * File:   sharder.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */

#ifndef SHARDER_H
#define	SHARDER_H
#include <iostream>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <vector>
#include <omp.h>
#include <errno.h>
#include <sstream>
#include <string>
#include <stdint.h>
#include "util/shardfilenames.h"
#include "../../util/logger.h"
#include "../io/cache.h"
#include "../io/ioutil.h"
#include "../../util/cmdopts.h"
namespace dsm {

    typedef uint32_t vid_t;
    //64M
#define SHARDER_BUFSIZE (64 * 1024 * 1024)

    template <typename EdgeDataType>
    struct edge_with_value {
        vid_t src;
        vid_t dst;
        EdgeDataType value; //weight

        edge_with_value() {
        }

        edge_with_value(vid_t src, vid_t dst, EdgeDataType value) : src(src), dst(dst), value(value) {

        }

        // Order primarily by dst, then by src

        bool operator<(edge_with_value<EdgeDataType> &x2) {
            return (dst < x2.dst);
        }

        bool stopper() {
            return src == 0 && dst == 0;
        }
    };

    extern std::map<int, size_t> shard_numedges;
    extern std::map<vid_t, vid_t> src_outnumedges;
    extern vid_t sumedges;
    extern vid_t max_vertexid;

    template <typename EdgeDataType>
    struct shard_flushinfo {
        //这个文件什么意思，预处理的中间文件
        std::string shovelname;
        //边数
        size_t numedges;
        //边-值缓存
        edge_with_value<EdgeDataType> * buffer;
        vid_t max_vertex;

        shard_flushinfo(std::string shovelname, vid_t max_vertex, size_t numedges, edge_with_value<EdgeDataType> * buffer) :
        shovelname(shovelname), numedges(numedges), buffer(buffer), max_vertex(max_vertex) {
        }

        void flush() {
            //将排序好的边及数据写入文件
            int f = open(shovelname.c_str(), O_WRONLY | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
            writea(f, buffer, numedges * sizeof (edge_with_value<EdgeDataType>));
            close(f);
            free(buffer);
        }
    };

    // Run in a thread

    template <typename EdgeDataType>
    static void * shard_flush_run(void * _info) {
        shard_flushinfo<EdgeDataType> * task = (shard_flushinfo<EdgeDataType>*)_info;
        task->flush();
        return NULL;
    }

    template <typename EdgeDataType>
    struct shovel_to_shard {
        //buffer的字节大小
        size_t bufsize_bytes;
        //buffer的边个数
        size_t bufsize_edges;
        std::string shovelfile;
        //shovel索引标记
        size_t idx;
        size_t bufidx;
        //边-值buffer
        edge_with_value<EdgeDataType> * buffer;
        //文件标识符
        int f;
        //边数
        size_t numedges;

        shovel_to_shard(size_t bufsize_bytes, std::string shovelfile) : bufsize_bytes(bufsize_bytes),
        shovelfile(shovelfile), idx(0), bufidx(0) {
            assert(bufsize_bytes % sizeof (edge_with_value<EdgeDataType>) == 0);
            f = open(shovelfile.c_str(), O_RDONLY);

            if (f < 0) {
                logstream(LOG_ERROR) << "Could not open shovel file: " << shovelfile << std::endl;
                printf("Error: %d, %s\n", errno, strerror(errno));
            }

            assert(f >= 0);

            buffer = (edge_with_value<EdgeDataType> *) malloc(bufsize_bytes);
            numedges = (get_filesize(shovelfile) / sizeof (edge_with_value<EdgeDataType>));
            bufsize_edges = (bufsize_bytes / sizeof (edge_with_value<EdgeDataType>));
            load_next();
        }

        virtual ~shovel_to_shard() {
            if (buffer != NULL) free(buffer);
            buffer = NULL;
        }

        void finish() {
            close(f);
            //移除文件或者目录
            remove(shovelfile.c_str());

            free(buffer);
            buffer = NULL;
        }

        void load_next() {
            size_t len = std::min(bufsize_bytes, ((numedges - idx) * sizeof (edge_with_value<EdgeDataType>)));
            //preada里面buffer指针不跟着移动，它的副本跟着移动
            preada(f, buffer, len, idx * sizeof (edge_with_value<EdgeDataType>));
            bufidx = 0;
        }

        bool has_more() {
            return idx < numedges;
        }

        edge_with_value<EdgeDataType> next() {
            if (bufidx == bufsize_edges) {
                load_next();
            }
            idx++;
            if (idx == numedges) {
                edge_with_value<EdgeDataType> x = buffer[bufidx++];
                finish();
                return x;
            }
            return buffer[bufidx++];
        }
    };

    template <typename EdgeDataType>
    class sharder {
        typedef edge_with_value<EdgeDataType> edge_t;

    protected:
        //graphchi.txt
        std::string basefilename;
        //最大顶点值
        vid_t max_vertex_id;

        /* Sharding */
        int nshards;
        //this_interval_start, (shardnum == nshards - 1 ? max_vertex_id : prevvid
        //每个键值对代表每个interval顶点区间
        std::vector< std::pair<vid_t, vid_t> > intervals;

        size_t curshovel_idx;
        //shovel大小
        size_t shovelsize;
        //shovel个数
        int numshovels;
        //预处理中间文件边数
        size_t shoveled_edges;

        edge_with_value<EdgeDataType> * curshovel_buffer;
        std::vector<pthread_t> shovelthreads;
        std::vector<shard_flushinfo<EdgeDataType> *> shoveltasks;

    public:

        sharder(std::string basefilename) : basefilename(basefilename) {

            curshovel_buffer = NULL;
        }

        virtual ~sharder() {
            if (curshovel_buffer == NULL) free(curshovel_buffer);
        }

        /**
         * Call to start a preprocessing session.
         */
        void start_preprocessing() {
            numshovels = 0;
            //16M                                                                               //12字节  
            shovelsize = (1024l * 1024l * (size_t) (get_config_option_int("MENBUDGET_MB", 1024)) / 4l / sizeof (edge_with_value<EdgeDataType>));
            curshovel_idx = 0;

            logstream(LOG_INFO) << "Starting preprocessing, shovel size: " << shovelsize * sizeof (edge_with_value<EdgeDataType>) << "bytes" << std::endl;

            curshovel_buffer = (edge_with_value<EdgeDataType> *) calloc(shovelsize, sizeof (edge_with_value<EdgeDataType>));

            assert(curshovel_buffer != NULL);

            shovelthreads.clear();

            /* Write the maximum vertex id place holder - to be filled later */
            max_vertex_id = 0;
            shoveled_edges = 0;
        }

        /**
         * Call to finish the preprocessing session.
         */
        void end_preprocessing() {
            flush_shovel(false);
        }

        void flush_shovel(bool async = true) {
            /* Flush in separate thread unless the last one */
            shard_flushinfo<EdgeDataType> * flushinfo = new shard_flushinfo<EdgeDataType>(shovel_filename(numshovels), max_vertex_id, curshovel_idx, curshovel_buffer);
            shoveltasks.push_back(flushinfo);

            if (!async) {
                curshovel_buffer = NULL;
                flushinfo->flush();

                /* Wait for threads to finish */
                logstream(LOG_INFO) << "Waiting shoveling threads..." << std::endl;
                for (int i = 0; i < (int) shovelthreads.size(); i++) {
                    pthread_join(shovelthreads[i], NULL);
                }
            } else {
                if (shovelthreads.size() > 2) {
                    logstream(LOG_INFO) << "Too many outstanding shoveling threads..." << std::endl;

                    for (int i = 0; i < (int) shovelthreads.size(); i++) {
                        pthread_join(shovelthreads[i], NULL);
                    }
                    shovelthreads.clear();
                }
                //为什么要创建这个buffer呢？，目的是清零
                logstream(LOG_INFO) << "create curshovel_buffer" << std::endl;
                curshovel_buffer = (edge_with_value<EdgeDataType> *) calloc(shovelsize, sizeof (edge_with_value<EdgeDataType>));
                pthread_t t;
                int ret = pthread_create(&t, NULL, shard_flush_run<EdgeDataType>, (void*) flushinfo);
                shovelthreads.push_back(t);
                assert(ret >= 0);
            }
            numshovels++;
            curshovel_idx = 0;
        }

        /**
         * Add edge to be preprocessed with a value.
         */
        //预处理时给边加值

        void preprocessing_add_edge(vid_t from, vid_t to, EdgeDataType val) {
            //  if (from == to) {
            //      // Do not allow self-edges
            //     return;
            //  } //源顶点  目的顶点   边值
            edge_with_value<EdgeDataType> e(from, to, val);

            // logstream(LOG_INFO) << "curshovel_idx"<<curshovel_idx << std::endl;
            //  logstream(LOG_INFO) << "shovelsize"<<curshovel_idx << std::endl;
            curshovel_buffer[curshovel_idx++] = e;

            if (curshovel_idx == shovelsize) {

                logstream(LOG_DEBUG) << "flush_shovel " << std::endl;
                flush_shovel();
            }
            max_vertex_id = std::max(std::max(from, to), max_vertex_id);
        }

        /**
         * Add edge without value to be preprocessed
         */
        void preprocessing_add_edge(vid_t from, vid_t to) {
            preprocessing_add_edge(from, to, EdgeDataType());
        }

        /**
         * Executes sharding.
         * @param nshards_string the number of shards as a number, or "auto" for automatic determination
         */
        int execute_sharding(std::string nshards_string) {
            //度量就该这样用
            logstream(LOG_INFO) << "execute_sharding)" << std::endl;

            determine_number_of_shards(nshards_string);

            write_shards();

            /* Print metrics */

            return nshards;
        }

        /**
         * Sharding. This code might be hard to read - modify very carefully!
         */
    protected:

        virtual void determine_number_of_shards(std::string nshards_string) {
            /* Count shoveled edges */
            shoveled_edges = 0;
            for (int i = 0; i < (int) shoveltasks.size(); i++) {
                shoveled_edges += shoveltasks[i]->numedges;
                delete shoveltasks[i];
            }

            if (nshards_string.find("auto") != std::string::npos || nshards_string == "0") {
                logstream(LOG_INFO) << "Determining number of shards automatically." << std::endl;

                int membudget_mb = get_config_option_int("MENBUDGET_MB", 1024);
                logstream(LOG_INFO) << "Assuming available memory is " << membudget_mb << " megabytes. " << std::endl;
                logstream(LOG_INFO) << " (This can be defined with configuration parameter 'membudget_mb')" << std::endl;

                size_t numedges = shoveled_edges;
                //100M
                double max_shardsize = membudget_mb * 1024. * 1024. / 8;
                logstream(LOG_INFO) << "Determining maximum shard size: " << (max_shardsize / 1024. / 1024.) << " MB." << std::endl;
                //设定shard个数
                nshards = (int) (1 + (numedges * (sizeof (vid_t)*2 + sizeof (EdgeDataType)) / max_shardsize) + 0.5);

            } else {
                nshards = atoi(nshards_string.c_str());
            }
            assert(nshards > 0);
            logstream(LOG_INFO) << "Number of shards to be created: " << nshards << std::endl;
        }


    protected:

        std::string shovel_filename(int idx) {
            std::stringstream ss;
            ss << basefilename << sizeof (EdgeDataType) << "." << idx << ".shovel";
            return ss.str();
        }


        //一个shard的边大小 类型是 edge_with_value

        void finish_shard(int shard, edge_t * shovelbuf, size_t shovelsize) {

            logstream(LOG_INFO) << "Starting final processing for shard: " << shard << std::endl;


            //shard 块目录文件 compressed_block_size是每个block的大小
            std::string shardname = filename_shard_edata_block(basefilename, shard, nshards);

            //numedges  一个shovel文件的边数
            size_t numedges = shovelsize / sizeof (edge_t);

            sumedges += numedges;

            shard_numedges.insert(std::map<int, size_t>::value_type(shard, numedges));

            logstream(LOG_DEBUG) << "Shovel size:" << shovelsize << " edges: " << numedges << std::endl;


            // Create the final file
            int f = open(shardname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0777);
            if (f < 0) {
                //logstream(LOG_ERROR) << "Could not open " << shardname << " error: " << strerror(errno) << std::endl;
                //logstream(LOG_DEBUG) << "mkdir _blockdir_ ..." << std::endl;
                std::string blockdirname = dirname_shard_edata_block(basefilename, nshards);
                mkdir(blockdirname.c_str(), 0777);
                f = open(shardname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0777);
            }
            assert(f >= 0);
            for (int i = 0; i < 10; i++) {
                std::cout << " src  " << shovelbuf[i].src << " dst " << shovelbuf[i].dst << " value  " << shovelbuf[i].value << std::endl;
            }
            writea(f, shovelbuf, shovelsize);
            //   bwrite_edata<FinalEdgeDataType>(ebuf, ebufptr, FinalEdgeDataType(edge.value), tot_edatabytes, edfname, edgecounter);


            close(f);
        }

        size_t edges_per_shard;
        //当前shard计数
        size_t cur_shard_counter;
        //shard 容量
        size_t shard_capacity;
        //shard的边数
        size_t sharded_edges;
        //shard id
        int shardnum;
        edge_with_value<EdgeDataType> * shardbuffer;
        //这个interval 开始的顶点id
        vid_t this_interval_start;

        void add(edge_with_value<EdgeDataType> val) {
            if (cur_shard_counter >= edges_per_shard) {
                createnextshard();
            }

            if (cur_shard_counter == shard_capacity) {
                /* Really should have a way to limit shard sizes, but probably not needed in practice */
                logstream(LOG_WARNING) << "Shard " << shardnum << " overflowing! " << cur_shard_counter << " / " << shard_capacity << std::endl;
                shard_capacity = (size_t) (1.2 * shard_capacity);
                shardbuffer = (edge_with_value<EdgeDataType>*) realloc(shardbuffer, shard_capacity * sizeof (edge_with_value<EdgeDataType>));
            }

            shardbuffer[cur_shard_counter++] = val;
            sharded_edges++;
        }

        void createnextshard() {
            assert(shardnum < nshards);
            intervals.push_back(std::pair<vid_t, vid_t>(this_interval_start, (shardnum == nshards - 1 ? max_vertex_id : shardbuffer[cur_shard_counter - 1].src)));
            this_interval_start = shardbuffer[cur_shard_counter - 1].src;
            finish_shard(shardnum++, shardbuffer, cur_shard_counter * sizeof (edge_with_value<EdgeDataType>));
            shardbuffer = (edge_with_value<EdgeDataType> *) malloc(shard_capacity * sizeof (edge_with_value<EdgeDataType>));
            cur_shard_counter = 0;
            // Adjust edges per hard so that it takes into account how many edges have been spilled now
            logstream(LOG_INFO) << "Remaining edges: " << (shoveled_edges - sharded_edges) << " remaining shards:" << (nshards - shardnum)
                    << " edges per shard=" << edges_per_shard << std::endl;
            if (shardnum < nshards) edges_per_shard = (shoveled_edges - sharded_edges) / (nshards - shardnum);
            logstream(LOG_INFO) << "Edges per shard: " << edges_per_shard << std::endl;

        }

        void done() {
            createnextshard();
            if (shoveled_edges != sharded_edges) {
                logstream(LOG_INFO) << "Shoveled " << shoveled_edges << " but sharded " << sharded_edges << " edges" << std::endl;
            }

            logstream(LOG_INFO) << "Created " << shardnum << " shards, for " << sharded_edges << " edges"<<std::endl;
            assert(shardnum <= nshards);
            free(shardbuffer);
            shardbuffer = NULL;

            max_vertexid = max_vertex_id;
            logstream(LOG_INFO) << "max_vertexid :" << max_vertexid << std::endl;
            logstream(LOG_INFO) << "sumedges :" << sumedges << std::endl;

            /* Write intervals */
            std::string fname = filename_intervals(basefilename, nshards);
            FILE * f = fopen(fname.c_str(), "w");

            if (f == NULL) {
                logstream(LOG_ERROR) << "Could not open file: " << fname << " error: " <<
                        strerror(errno) << std::endl;
            }
            assert(f != NULL);
            for (int i = 0; i < (int) intervals.size(); i++) {
                fprintf(f, "%u\n", intervals[i].second);
            }
            fclose(f);
            //顶点个数
            /* Write meta-file with the number of vertices */
            std::string numv_filename = basefilename + ".numvertices";
            f = fopen(numv_filename.c_str(), "w");
            fprintf(f, "%u\n", 1 + max_vertex_id);
            fclose(f);
            //每个shard的边数
            std::string numedges_filename = basefilename + ".shardnumedges";
            f = fopen(numedges_filename.c_str(), "w");
            std::map<int, size_t>::const_iterator iter;
            for (iter = shard_numedges.begin(); iter != shard_numedges.end(); iter++) {
                fprintf(f, "%d  %lu\n", iter->first, iter->second);
            }
            
            fclose(f);
            
            std::string src_outnumedges_filename = basefilename + ".src_outnumedges";
            f = fopen(src_outnumedges_filename.c_str(), "w");
            std::map<vid_t, vid_t>::const_iterator _iter;
            for (_iter = src_outnumedges.begin(); _iter != src_outnumedges.end(); _iter++) {
                fprintf(f, "%d  %u\n", _iter->first, _iter->second);
            }
            std::cout<<"write src_outnumedges   over"<<std::endl;
            //sleep(10);
            fclose(f);
        }

        /* End: Kway -merge sink interface */

        /**
         * Write the shard by sorting the shovel file and compressing the
         * adjacency information.
         * To support different shard types, override this function!
         */
        void write_shards() {

            size_t membudget_mb = (size_t) get_config_option_int("MENBUDGET_MB", 1024);

            sharded_edges = 0;
            edges_per_shard = shoveled_edges / nshards + 1;
            shard_capacity = edges_per_shard / 2 * 3; // Shard can go 50% over
            shardnum = 0;
            this_interval_start = 0;
            shardbuffer = (edge_with_value<EdgeDataType> *) calloc(shard_capacity, sizeof (edge_with_value<EdgeDataType>));
            logstream(LOG_INFO) << "Edges per shard: " << edges_per_shard << " nshards=" << nshards << " total: " << shoveled_edges << std::endl;
            cur_shard_counter = 0;


            /* Initialize kway merge sources */
            size_t B = membudget_mb * 1024 * 1024 / 2 / numshovels;
            while (B % sizeof (edge_with_value<EdgeDataType>) != 0) B++;
            logstream(LOG_INFO) << "Buffer size in merge phase: " << B << std::endl;
            int i = 0;
            while (i < numshovels) {
                // if (i < numshovels) 
                shovel_to_shard<EdgeDataType>* create_shard = new shovel_to_shard<EdgeDataType>(B, shovel_filename(i));

                while (create_shard->has_more()) {
                    add(create_shard->next());
                }
                i++;
            }

            done();

        }


    }; // End class sharder


}; // namespace



#endif	/* SHARDER_H */

