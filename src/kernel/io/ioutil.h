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
#ifndef IOUTIL_H
#define IOUTIL_H

#include <unistd.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include "../../util/logger.h"
#include <fcntl.h>

namespace dsm
{
// Reads given number of bytes to a buffer
template <typename T>
void preada(int f, T * tbuf, size_t nbytes, size_t off) {
    size_t nread = 0;
    char * buf = (char*)tbuf;
    while(nread<nbytes) {
        /*
         * 功能：带偏移量地原子的从文件中读取数据 
返回值：成功，返回成功读取数据的字节数；失败，出错返回-1； 
         * 
         * 注意，实际作用与read函数+lseek函数作用相同，区别是pread执行后，
         * 文件偏移指针不变，且pread是原子操作，无法中断其定位和读操作。


         */
        ssize_t a = pread(f, buf, nbytes - nread, off + nread);
        if (a == (-1)) {
            std::cout << "Error, could not read: " << strerror(errno) << "; file-desc: " << f << std::endl;
            std::cout << "Pread arguments: " << f << " tbuf: " << tbuf << " nbytes: " << nbytes << " off: " << off << std::endl;
            assert(a != (-1));
        }
        assert(a>0);
        buf += a;
        nread += a;
    }
    assert(nread <= nbytes);
}

template <typename T>
void preada_trunc(int f, T * tbuf, size_t nbytes, size_t off) {
    size_t nread = 0;
    char * buf = (char*)tbuf;
    while(nread<nbytes) {
        size_t a = pread(f, buf, nbytes-nread, off+nread);
        if (a == 0) {
            // set rest to 0
     //       std::cout << "WARNING: file was not long enough - filled with zeros. " << std::endl;
            memset(buf, 0, nbytes-nread);
            return;
        }
        buf += a;
        nread += a;
    }

} 

template <typename T>
size_t readfull(int f, T ** buf) {
    /*
     * 每一个已打开的文件都有一个读写位置, 当打开文件时通常其读写位置是指向文件开头, 
     * 若是以附加的方式打开文件(如O_APPEND), 则读写位置会指向文件尾. 当read()或write()时,
     *  读写位置会随之增加,lseek()便是用来控制该文件的读写位置. 参数fildes 为已打开的文件描述词,
     *  参数offset 为根据参数whence来移动读写位置的位移数.
     * 
     * 返回值：当调用成功时则返回目前的读写位置, 也就是距离文件开头多少个字节. 若有错误则返回-1, errno 会存放错误代码.

     */
     off_t sz = lseek(f, 0, SEEK_END);
     lseek(f, 0, SEEK_SET);
     *buf = (T*)malloc(sz);
    preada(f, *buf, sz, 0);
    return sz;
}

template <typename T>
void pwritea(int f, T * tbuf, size_t nbytes, size_t off) {
    size_t nwritten = 0;
    assert(f>0);
    char * buf = (char*)tbuf;
    while(nwritten<nbytes) {
        size_t a = pwrite(f, buf, nbytes-nwritten, off+nwritten);
        if (a == size_t(-1)) {
            logstream(LOG_ERROR) << "f:" << f << " nbytes: " << nbytes << " written: " << nwritten << " off:" << 
                off << " f: " << f << " error:" <<  strerror(errno) << std::endl;
            assert(false);
        }
        assert(a>0);
        buf += a;
        nwritten += a;
    }
}




template <typename T>
void checkarray_filesize(std::string fname, size_t nelements) {
    // Check the vertex file is correct size
    int f = open(fname.c_str(),  O_RDWR | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
    if (f < 1) {
        logstream(LOG_ERROR) << "Error initializing the data-file: " << fname << " error:" <<  strerror(errno) << std::endl;    }
    assert(f>0);
    int err = ftruncate(f, nelements * sizeof(T));
    if (err != 0) {
        logstream(LOG_ERROR) << "Error in adjusting file size: " << fname << " to size: " << nelements * sizeof(T)    
                 << " error:" <<  strerror(errno) << std::endl;
    }
    assert(err == 0);
    close(f);
}

template <typename T>
void writea(int f, T * tbuf, size_t nbytes) {
    size_t nwritten = 0;
    char * buf = (char*)tbuf;
    while(nwritten<nbytes) {
        size_t a = write(f, buf, nbytes-nwritten);
        assert(a>0);
        if (a == size_t(-1)) {
            logstream(LOG_ERROR) << "Could not write " << (nbytes-nwritten) << " bytes!" << " error:" <<  strerror(errno) << std::endl;
            assert(false);
        }
        buf += a;
        nwritten += a;
    }
    
}

template <typename T>
void reada(int f, T * tbuf, size_t nbytes) {
    size_t nread = 0;
    char * buf = (char*)tbuf;
    while(nread<nbytes) {
        ssize_t a = read(f, buf, nbytes - nread);
        if (a == (-1)) {
            std::cout << "Error, could not read: " << strerror(errno) << "; file-desc: " << f << std::endl;
            std::cout << "Pread arguments: " << f << " tbuf: " << tbuf << " nbytes: " << nbytes  << std::endl;
            assert(a != (-1));
        }
       // assert(a>0);
        buf += a;
        nread += a;
    }
    assert(nread <= nbytes);
}

}
#endif


