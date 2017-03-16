#include "statetable.h"
#include <map>
#include "preprocessing/sharder.h"
namespace dsm {


    double termcheck_threshold;

    double termcheck_interval;

    vector<KVVPAIR> thread_interval;

    // vector<SHARD_KVPAIR> thread_id;

    vid_t max_vertexid = 0;

    vid_t sumedges = 0;

    map<vid_t, vid_t> src_outnumedges;

    map<int, size_t> shard_numedges;

    map<pthread_t, vid_t> thread_id;

    map<pthread_t, vid_t> iothread_id;

    double total_time_1, total_time_2, total_time_3;

}