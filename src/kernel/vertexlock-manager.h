/* 
 * File:   lock-registry.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */

#ifndef LOCK_REGISTRY_H
#define	LOCK_REGISTRY_H
#include <pthread.h>
#include <limits.h>
#include <assert.h>
#include <stdio.h>
namespace dsm {

    //vertex lock

    class Vertexlock {
    public:

        Vertexlock(int _num_nodes) : num_nodes(_num_nodes) {

            mutex = new pthread_mutex_t[num_nodes];

        }

        ~Vertexlock() {
            delete [] mutex;
        }

        void Initmutex();

        void Destroymutex();

        void lock(int node);

        void unlock(int node);

        bool try_lock(int node);

    public:
        pthread_mutex_t * mutex;
        int num_nodes;

    };

    class cachelock {
    private:
        // mutable not actually needed
        mutable pthread_mutex_t m_mut;
    public:

        cachelock() {
            int error = pthread_mutex_init(&m_mut, NULL);
            assert(!error);
        }
        void lock() const;
        void unlock() const;
        bool try_lock() const;

        ~cachelock() {
            int error = pthread_mutex_destroy(&m_mut);
            if (error)
                perror("Error: failed to destroy mutex");
            assert(!error);
        }
    };


}


#endif	/* LOCK_REGISTRY_H */

