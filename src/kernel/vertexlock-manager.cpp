#include "vertexlock-manager.h"
#include<assert.h>
#include <iostream>
#include "../util/logger.h"
namespace dsm {

    void Vertexlock::Initmutex() {
        int error;
        for (int i = 0; i < num_nodes; i++) {
            error = pthread_mutex_init(&mutex[i], NULL);
            if (error)
                logstream(LOG_ERROR) << "Error: failed to init mutex" << std::endl;
            assert(!error);
        }

    }

    void Vertexlock::Destroymutex() {
        int error;
        for (int i = 0; i < num_nodes; i++) {
            error = pthread_mutex_destroy(&mutex[i]);
            if (error)
                logstream(LOG_ERROR) << "Error: failed to destroy mutex" << std::endl;
            assert(!error);
        }
    }

    void Vertexlock::lock(int node) {
        int error = pthread_mutex_lock(&mutex[node]);
        assert(!error);
    }

    void Vertexlock::unlock(int node) {
        int error = pthread_mutex_unlock(&mutex[node]);
        assert(!error);
    }

    bool Vertexlock::try_lock(int node) {
        return pthread_mutex_trylock(&mutex[node]) == 0;
    }

   

    void cachelock::lock() const {
        int error = pthread_mutex_lock(&m_mut);
        assert(!error);
    }

    void cachelock::unlock() const {
        int error = pthread_mutex_unlock(&m_mut);
        assert(!error);
    }

    bool cachelock::try_lock() const {
        return pthread_mutex_trylock(&m_mut) == 0;
    }

    


}