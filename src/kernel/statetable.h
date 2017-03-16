/* 
 * File:   statetable.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */

#ifndef STATETABLE_H
#define	STATETABLE_H

#include "../util/common.h"
#include "table.h"
#include "vertexlock-manager.h"
#include <boost/noncopyable.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/lognormal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <algorithm>



namespace dsm {

    static const int sample_size = 1000;

    //in_momery_mode

    struct KVVPAIR {
        pthread_t thread_id;
        int64_t interval_first;
        int64_t interval_last;
    };

    extern vector<KVVPAIR> thread_interval;

    extern map<pthread_t, vid_t> thread_id;

    template <class K, class V1, class V2, class V3>
    struct ClutterRecord;

    template <class K, class V1, class V2, class V3, class E>
    class StateTable :
    public TableBase,
    public Snapshottable,
    public TypedTable<K, V1, V2, V3>,
    private boost::noncopyable {
    private:
#pragma pack(push, 1)

        struct Bucket {
            K k;
            V1 v1; //delta
            V2 v2;
            vector<V3> v3;
            V1 priority;
            int edge_count;
            //use in hash
            bool in_use;
        };
#pragma pack(pop)

    public:

        struct Iterator : public TypedTableIterator<K, V1, V2, V3, E> {//2

            Iterator(StateTable<K, V1, V2, V3, E>& parent, int num_threads, bool bfilter, bool inmemmode) : pos(-1), parent_(parent), num_threads(num_threads), inmemmode(inmemmode) {
                pos = -1; //pos(-1) doesn't work
                /*
                 this filter is very important in large-scale experiment, if there is no such control,
                 * many useless parsing will occur.,degrading the performance a lot
             
                 *///check delta change
                if (bfilter) {
                    //check if there is a change
                    b_no_change = true;

                    //random number generator
                    //boost // 
                    boost::mt19937 gen(time(0));

                    boost::uniform_int<> dist(0, parent_.buckets_.size() - 1);

                    boost::variate_generator<boost::mt19937&, boost::uniform_int<> > rand_num(gen, dist);

                    defaultv = ((IterateKernel<K, V1, V3, E>*)parent_.info_.iterkernel)->default_v();
                    int i;
                    for (i = 0; i < sample_size && b_no_change; i++) {
                        int rand_pos = rand_num();
                        while (!parent_.buckets_[rand_pos].in_use) {
                            rand_pos = rand_num();
                        }

                        b_no_change = b_no_change && parent_.buckets_[rand_pos].v1 == defaultv;
                    }
                } else {
                    b_no_change = false;
                }
                if (inmemmode) {
                    int64_t normal_interval = parent_.buckets_.size() / num_threads;
                    logstream(LOG_INFO) << "normal_interval:" << normal_interval << std::endl;
                    // int64_t last_interval = scheduled_pos.size() % num_threads;


                    size_t i = 0;
                    size_t len = thread_interval.size();
                    for (; i < len - 1; i++) {

                        thread_interval[i].interval_first = normal_interval*i;
                        thread_interval[i].interval_last = normal_interval * (i + 1) - 1;

                        logstream(LOG_INFO) << "thread_interval[" << i << "].interval_first:" << thread_interval[i].interval_first << std::endl;
                        logstream(LOG_INFO) << "thread_interval" << i << "].interval_last:" << thread_interval[i].interval_last << std::endl;
                    }
                    //  logstream(LOG_INFO) << "i:" << i << std::endl;
                    thread_interval[i].interval_first = normal_interval*i;
                    thread_interval[i].interval_last = parent_.buckets_.size() - 1;


                    logstream(LOG_INFO) << "thread_interval[" << i << "].interval_first:" << thread_interval[i].interval_first << std::endl;
                    logstream(LOG_INFO) << "thread_interval" << i << "].interval_last:" << thread_interval[i].interval_last << std::endl;
                    //Next();
                }

            }

            bool is_in_use(int64_t pos) {

                return parent_.buckets_[pos].in_use;
            }

            int64_t buckets_size() {
                return parent_.size_;
            }

            int64_t vector_size(int64_t pos) {
                return parent_.buckets_[pos].v3.size();
            }

            void vector_clear(int64_t pos) {
                parent_.buckets_[pos].v3.clear();
            }

            int64_t thread_begin(pthread_t thread_id) {
                for (int i = 0; i < thread_interval.size(); i++) {
                    if (thread_interval[i].thread_id == thread_id)
                        return thread_interval[i].interval_first;
                }
                return -1;
            }

            int64_t thread_end(pthread_t thread_id) {
                for (int i = 0; i < thread_interval.size(); i++) {
                    if (thread_interval[i].thread_id == thread_id)
                        return thread_interval[i].interval_last;
                }
                return -1;
            }
            //external mode

            vid_t thread_begin_pos(pthread_t _thread_id) {

                map<pthread_t, vid_t>::const_iterator it = thread_id.find(_thread_id);

                if (it != thread_id.end()) {

                    return it->second;
                }
                return -1;
            }

            virtual ~Iterator() {
            }

            Marshal<K>* kmarshal() {
                return parent_.kmarshal();
            }

            Marshal<V1>* v1marshal() {
                return parent_.v1marshal();
            }

            Marshal<V2>* v2marshal() {
                return parent_.v2marshal();
            }

            bool Next() {
                do {
                    ++pos;
                } while (pos < parent_.size_ && (!parent_.buckets_[pos].in_use));

                if (pos >= parent_.size_) {
                    return false;
                } else {
                    return true;
                }
            }

            bool done() {
                return pos + 1 == parent_.size_;
            }

            const K& key() {
                return parent_.buckets_[pos].k;
            }

            const int& edge_count() {
                return parent_.buckets_[pos].edge_count;
            }

            V1& value1() {
                return parent_.buckets_[pos].v1;
            }

            V2& value2() {
                return parent_.buckets_[pos].v2;
            }

            vector<V3>& value3() {
                return parent_.buckets_[pos].v3;
            }

            const K& key(int64_t pos) {
                return parent_.buckets_[pos].k;
            }

            const int& edge_count(int64_t pos) {
                return parent_.buckets_[pos].edge_count;
            }

            V1& value1(int64_t pos) {
                return parent_.buckets_[pos].v1;
            }

            V2& value2(int64_t pos) {
                return parent_.buckets_[pos].v2;
            }
            

            vector<V3>& value3(int64_t pos) {
                return parent_.buckets_[pos].v3;
            }
            
            
            int64_t pos;
            int num_threads;
            StateTable<K, V1, V2, V3, E> &parent_;
            bool inmemmode;
            bool b_no_change;
            V1 defaultv;
        }; //2

        struct ScheduledIterator : public TypedTableIterator<K, V1, V2, V3, E> {//3

            ScheduledIterator(StateTable<K, V1, V2, V3, E>& parent, int num_threads, bool bfilter, bool inmemmode) : pos(-1), parent_(parent), num_threads(num_threads), inmemmode(inmemmode) {

                pos = -1;
                b_no_change = true;
                Timer times;
                //random number generator
                boost::mt19937 gen(time(0));
                boost::uniform_int<> dist(0, parent_.buckets_.size() - 1);
                boost::variate_generator<boost::mt19937&, boost::uniform_int<> > rand_num(gen, dist);

                V1 defaultv = ((IterateKernel<K, V1, V3, E>*)parent_.info_.iterkernel)->default_v();

                if (parent_.entries_ <= sample_size) {
                    //if table size is less than the sample set size, schedule them all
                    int i;
                    for (i = 0; i < parent_.size_; i++) {
                        if (parent_.buckets_[i].in_use) {
                            scheduled_pos.push_back(i);
                            b_no_change = b_no_change && parent_.buckets_[i].v1 == defaultv;
                        }
                    }
                    if (b_no_change && bfilter) return;
                    if (!bfilter) b_no_change = false;
                } else {
                    //sample random pos, the sample reflect the whole data set more or less
                    vector<int> sampled_pos;
                    int i;
                    // int trials = 0;
                    for (i = 0; i < sample_size; i++) {
                        int rand_pos = rand_num();
                        //trials++;
                        while (!parent_.buckets_[rand_pos].in_use) {

                            rand_pos = rand_num();
                            // trials++;
                        }
                        sampled_pos.push_back(rand_pos);

                        b_no_change = b_no_change && parent_.buckets_[rand_pos].v1 == defaultv;
                    }

                    if (b_no_change && bfilter) return;
                    if (!bfilter) b_no_change = false;

                    //get the cut index, everything larger than the cut will be scheduled
                    sort(sampled_pos.begin(), sampled_pos.end(), compare_priority(parent_));

                    int cut_index = sample_size * parent_.info_.schedule_portion;
                    //theshold
                    V1 threshold = parent_.buckets_[sampled_pos[cut_index]].priority;
                    logstream(LOG_INFO) << "cut index " << cut_index << " theshold " << threshold << " pos " << sampled_pos[cut_index] << " max " << parent_.buckets_[sampled_pos[cut_index]].v1 << " thread id " << std::endl;
                    //  sleep(2);
                    if (cut_index == 0 || parent_.buckets_[sampled_pos[0]].priority == threshold) {
                        //to avoid non eligible records
                        int i;
                        for (i = 0; i < parent_.size_; i++) {
                            if (!parent_.buckets_[i].in_use) continue;
                            if (parent_.buckets_[i].v1 == defaultv) continue;

                            if (parent_.buckets_[i].priority >= threshold) {
                                scheduled_pos.push_back(i);
                            }
                        }
                        // logstream(LOG_INFO) << "cut index 0== threshold" << std::endl;
                    } else {
                        int i;
                        for (i = 0; i < parent_.size_; i++) {
                            if (!parent_.buckets_[i].in_use) continue;
                            if (parent_.buckets_[i].v1 == defaultv) continue;

                            if (parent_.buckets_[i].priority > threshold) {
                                scheduled_pos.push_back(i);
                            }
                        }
                    }
                    // logstream(LOG_INFO) << "cut index 0!= threshold" << std::endl;

                }

                logstream(LOG_INFO) << "table size " << parent_.buckets_.size() << " scheduled  size" << scheduled_pos.size() << std::endl;

                if (inmemmode) {
                    // cout << "scheduled_pos.size:" << scheduled_pos.size() << std::endl;
                    int64_t normal_interval = scheduled_pos.size() / num_threads;
                    logstream(LOG_INFO) << "normal_interval:" << normal_interval << std::endl;
                    // int64_t last_interval = scheduled_pos.size() % num_threads;


                    size_t i = 0;
                    size_t len = thread_interval.size();
                    for (; i < len - 1; i++) {

                        thread_interval[i].interval_first = normal_interval*i;
                        thread_interval[i].interval_last = normal_interval * (i + 1) - 1;

                        logstream(LOG_INFO) << "thread_interval[" << i << "].interval_first:" << thread_interval[i].interval_first << std::endl;
                        logstream(LOG_INFO) << "thread_interval" << i << "].interval_last:" << thread_interval[i].interval_last << std::endl;
                    }
                    //  cout << "i:" << i << std::endl;
                    thread_interval[i].interval_first = normal_interval*i;
                    thread_interval[i].interval_last = scheduled_pos.size() - 1;


                    logstream(LOG_INFO) << "thread_interval[" << i << "].interval_first:" << thread_interval[i].interval_first << std::endl;
                    logstream(LOG_INFO) << "thread_interval" << i << "].interval_last:" << thread_interval[i].interval_last << std::endl;
                    //Next();
                }
            }

            bool is_in_use(int64_t pos) {

                return parent_.buckets_[pos].in_use;
            }

            int64_t buckets_size() {

            }

            void vector_clear(int64_t pos) {
            }

            int64_t vector_size(int64_t pos) {

            }

            virtual ~ScheduledIterator() {
            }

            Marshal<K>* kmarshal() {
                return parent_.kmarshal();
            }

            Marshal<V1>* v1marshal() {
                return parent_.v1marshal();
            }

            Marshal<V2>* v2marshal() {
                return parent_.v2marshal();
            }

            int64_t thread_begin(pthread_t thread_id) {
                for (int i = 0; i < thread_interval.size(); i++) {
                    if (thread_interval[i].thread_id == thread_id)
                        return thread_interval[i].interval_first;
                }
                return -1;
            }

            int64_t thread_end(pthread_t thread_id) {
                for (int i = 0; i < thread_interval.size(); i++) {
                    if (thread_interval[i].thread_id == thread_id)
                        return thread_interval[i].interval_last;
                }
                return -1;
            }
            //external mode

            vid_t thread_begin_pos(pthread_t _thread_id) {

                map<pthread_t, vid_t>::const_iterator it = thread_id.find(_thread_id);

                if (it != thread_id.end()) {

                    return it->second;
                }
                return -1;
            }

            bool Next() {
                ++pos;
                return true;
            }

            bool done() {
                return pos + 1 == scheduled_pos.size();
            }

            const K& key() {
                return parent_.buckets_[scheduled_pos[pos]].k;
            }

            const int& edge_count() {
                return parent_.buckets_[scheduled_pos[pos]].edge_count;
            }

            V1& value1() {
                return parent_.buckets_[scheduled_pos[pos]].v1;
            }

            V2& value2() {
                return parent_.buckets_[scheduled_pos[pos]].v2;
            }

            vector<V3>& value3() {
                return parent_.buckets_[scheduled_pos[pos]].v3;
            }

            const K& key(int64_t pos) {
                return parent_.buckets_[scheduled_pos[pos]].k;
            }

            const int& edge_count(int64_t pos) {
                return parent_.buckets_[scheduled_pos[pos]].edge_count;
            }

            V1& value1(int64_t pos) {
                return parent_.buckets_[scheduled_pos[pos]].v1;
            }

            V2& value2(int64_t pos) {
                return parent_.buckets_[scheduled_pos[pos]].v2;
            }

            vector<V3>& value3(int64_t pos) {
                return parent_.buckets_[scheduled_pos[pos]].v3;
            }

            class compare_priority {
            public:
                StateTable<K, V1, V2, V3, E> &parent;

                compare_priority(StateTable<K, V1, V2, V3, E> &inparent) : parent(inparent) {
                }

                bool operator()(const int a, const int b) {
                    return parent.buckets_[a].priority > parent.buckets_[b].priority;

                }
            };

            int64_t pos;
            int num_threads;
            StateTable<K, V1, V2, V3, E> &parent_;
            bool inmemmode;
            double portion;
            vector<int> scheduled_pos;
            bool b_no_change;
        };

        //for termination check

        struct EntirePassIterator : public TypedTableIterator<K, V1, V2, V3, E>, public LocalTableIterator<K, V2> {//4

            EntirePassIterator(StateTable<K, V1, V2, V3, E>& parent) : pos(-1), parent_(parent) {
                //Next();
                total = 0;
                pos = -1;
                defaultv = ((IterateKernel<K, V1, V3, E>*)parent_.info_.iterkernel)->default_v();
            }

            bool is_in_use(int64_t pos) {

                return parent_.buckets_[pos].in_use;
            }

            int64_t buckets_size() {

            }

            int64_t vector_size(int64_t pos) {

            }

            void vector_clear(int64_t pos) {
            }

            int64_t thread_begin(pthread_t thread_id) {
            }

            int64_t thread_end(pthread_t thread_id) {
            }

            vid_t thread_begin_pos(pthread_t _thread_id) {

            }

            virtual ~EntirePassIterator() {
            }

            Marshal<K>* kmarshal() {
                return parent_.kmarshal();
            }

            Marshal<V1>* v1marshal() {
                return parent_.v1marshal();
            }

            Marshal<V2>* v2marshal() {
                return parent_.v2marshal();
            }

            bool Next() {
                do {
                    ++pos;
                } while (pos < parent_.size_ && !parent_.buckets_[pos].in_use);
                total++;

                if (pos >= parent_.size_) {
                    return false;
                } else {
                    return true;
                }
            }

            bool done() {
                //cout<< "entire pos " << pos << "\tsize" << parent_.size_ << endl;
                return pos + 1 == parent_.size_;
            }

            V1 defaultV() {
                return defaultv;
            }

            const K& key() {
                return parent_.buckets_[pos].k;
            }

            const int& edge_count() {
                return parent_.buckets_[pos].edge_count;
            }

            V1& value1() {
                return parent_.buckets_[pos].v1;
            }

            V2& value2() {
                return parent_.buckets_[pos].v2;
            }

            vector<V3>& value3() {
                return parent_.buckets_[pos].v3;
            }

            const K& key(int64_t pos) {
                return parent_.buckets_[pos].k;
            }

            const int& edge_count(int64_t pos) {
                return parent_.buckets_[pos].edge_count;
            }

            V1& value1(int64_t pos) {
                return parent_.buckets_[pos].v1;
            }

            V2& value2(int64_t pos) {
                return parent_.buckets_[pos].v2;
            }

            vector<V3>& value3(int64_t pos) {
                return parent_.buckets_[pos].v3;
            }

            int64_t pos;
            StateTable<K, V1, V2, V3, E> &parent_;
            int total;
            V1 defaultv;
        }; //4

        struct Factory : public TableFactory {

            TableBase* New() {
                return new StateTable<K, V1, V2, V3, E>();
            }
        };

        // Construct a StateTable with the given initial size; it will be expanded as necessary.
        StateTable(int size = 1);

        ~StateTable() {
        }

        void Init(const TableDescriptor* td) {
            TableBase::Init(td);
        }

        V1 getF1(const K& k);
        V2 getF2(const K& k);
        vector<V3> getF3(const K& k);
        ClutterRecord<K, V1, V2, V3> get(const K& k);
        bool contains(const K& k);
        void put(const K& k, const int &edge_count, const V1& v1, const V2& v2, const vector<V3>& v3);
        void updateF1(const K& k, const V1& v);
        void updateF2(const K& k, const V2& v);
        void updateF3(const K& k, const V3& v);
        void clearF3(const K&k);
        void accumulateF1(const K& k, const V1& v);
        void accumulateF2(const K& k, const V2& v);
        void accumulateF3(const K& k, const vector<V3>& v);

        bool remove(const K& k) {
            logstream(LOG_FATAL) << "Not implemented." << std::endl;
            return false;
        }

        void resize(int64_t size);

        bool empty() {
            return size() == 0;
        }

        int64_t size() {
            return entries_;
        }

        void clear() {
            for (int i = 0; i < size_; ++i) {
                buckets_[i].in_use = 0;
            }
            entries_ = 0;
        }

        void reset() {
        }

        bool compare_priority(int i, int j) {
            return buckets_[i].priority > buckets_[j].priority;

        }

        TableIterator *get_iterator(int num_threads, bool bfilter, bool inmemmode) {

            if (terminated_) return NULL; //if get term signal, return null to tell program terminate

            sleep(0.1);

            Iterator* iter = new Iterator(*this, num_threads, bfilter, inmemmode);

            while (iter->b_no_change) {
                logstream(LOG_INFO) << "wait for put" << std::endl;

                delete iter;

                sleep(0.1);


                if (terminated_) return NULL; //if get term signal, return null to tell program terminate

                iter = new Iterator(*this, num_threads, bfilter, inmemmode);


            }

            return iter;
        }

        TableIterator *schedule_iterator(int num_threads, bool bfilter, bool inmemmode) {

            if (terminated_) return NULL;

            sleep(0.1);

            ScheduledIterator* iter = new ScheduledIterator(*this, num_threads, bfilter, inmemmode);
            while (iter->b_no_change) {
                logstream(LOG_INFO) << "wait for put, send buffered updates";

                delete iter;

                sleep(0.1);

                if (terminated_) return NULL; //if get term signal, return null to tell program terminate

                iter = new ScheduledIterator(*this, num_threads, bfilter, inmemmode);

            }

            return iter;
        }

        TableIterator *entirepass_iterator() {
            return new EntirePassIterator(*this);
        }

        void serializeToSnapshot(long *updates, double *totalF2);

        Marshal<K>* kmarshal() {
            return ((Marshal<K>*)info_.key_marshal);
        }

        Marshal<V1>* v1marshal() {
            return ((Marshal<V1>*)info_.value1_marshal);
        }

        Marshal<V2>* v2marshal() {
            return ((Marshal<V2>*)info_.value2_marshal);
        }

        Marshal<V3>* v3marshal() {
            return ((Marshal<V3>*)info_.value3_marshal);
        }

    private:

        uint32_t bucket_idx(K k) {
            return hashobj_(k) % size_;
        }

        int bucket_for_key(const K& k) {
            int start = bucket_idx(k);
            int b = start;

            do {
                if (buckets_[b].in_use) {
                    if (buckets_[b].k == k) {
                        return b;
                    }
                } else {
                    return -1;
                }

                b = (b + 1) % size_;
            } while (b != start);

            return -1;
        }
        //Bucket集合
        std::vector<Bucket> buckets_;

        int64_t entries_;
        int64_t size_;
        double total_curr;
        int64_t total_updates;

        std::tr1::hash<K> hashobj_;

    }; //1

    template <class K, class V1, class V2, class V3, class E>
    StateTable<K, V1, V2, V3, E>::StateTable(int size)
    : buckets_(0), entries_(0), size_(0), total_curr(0), total_updates(0) {
        clear();

        logstream(LOG_INFO) << "create new statetable  " << std::endl;
        resize(size);
    }

    //it can also be used to generate snapshot, but currently in order to measure the performance we skip this step, 
    //but focus on termination check

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::serializeToSnapshot(long* updates, double* totalF2) {
        total_curr = 0;
        EntirePassIterator* entireIter = new EntirePassIterator(*this);
        total_curr = static_cast<double> (((TermChecker<K, V2>*)info_.termchecker)->estimate_prog(entireIter));
        delete entireIter;
        *updates = total_updates;
        *totalF2 = total_curr;
    }

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::resize(int64_t size) {
        assert(size > 0);
        if (size_ == size)
            return;

        std::vector<Bucket> old_b = buckets_;

        buckets_.resize(size);
        size_ = size;
        clear();

        for (int i = 0; i < old_b.size(); ++i) {
            if (old_b[i].in_use) {
                put(old_b[i].k, old_b[i].edge_count, old_b[i].v1, old_b[i].v2, old_b[i].v3);

            }
        }


    }

    template <class K, class V1, class V2, class V3, class E>
    bool StateTable<K, V1, V2, V3, E>::contains(const K& k) {
        return bucket_for_key(k) != -1;
    }

    template <class K, class V1, class V2, class V3, class E>
    V1 StateTable<K, V1, V2, V3, E>::getF1(const K& k) {
        int b = bucket_for_key(k);
        //The following key display is a hack hack hack and only yields valid
        //results for ints.  It will display nonsense for other types.
        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << std::endl;
        assert(b != -1);
        return buckets_[b].v1;
    }

    template <class K, class V1, class V2, class V3, class E>
    V2 StateTable<K, V1, V2, V3, E>::getF2(const K& k) {
        int b = bucket_for_key(k);
        //The following key display is a hack hack hack and only yields valid
        //results for ints.  It will display nonsense for other types.
        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << std::endl;
        assert(b != -1);
        return buckets_[b].v2;
    }

    template <class K, class V1, class V2, class V3, class E>
    vector<V3> StateTable<K, V1, V2, V3, E>::getF3(const K& k) {
        int b = bucket_for_key(k);
        //The following key display is a hack hack hack and only yields valid
        //results for ints.  It will display nonsense for other types.
        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << std::endl;
        assert(b != -1);
        return buckets_[b].v3;
    }

    template <class K, class V1, class V2, class V3, class E>
    ClutterRecord<K, V1, V2, V3> StateTable<K, V1, V2, V3, E>::get(const K& k) {
        int b = bucket_for_key(k);
        //The following key display is a hack hack hack and only yields valid
        //results for ints.  It will display nonsense for other types.
        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << std::endl;
        assert(b != -1);

        return ClutterRecord<K, V1, V2, V3>(k, buckets_[b].v1, buckets_[b].v2, buckets_[b].v3);
    }

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::updateF1(const K& k, const V1& v) {
        int b = bucket_for_key(k);

        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << std::endl;
        assert(b != -1);
        // lock(this->shard());
        buckets_[b].v1 = v;
        buckets_[b].priority = 0; //didn't use priority function, assume the smallest priority is 0
        total_updates++;
        //unlock(this->shard());
    }

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::updateF2(const K& k, const V2& v) {
        int b = bucket_for_key(k);

        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << std::endl;
        assert(b != -1);

        buckets_[b].v2 = v;
    }

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::updateF3(const K& k, const V3& v) {
        int b = bucket_for_key(k);

        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << std::endl;
        assert(b != -1);
        if(buckets_[b].v3.size()!=buckets_[b].edge_count)
        buckets_[b].v3.push_back(v);
    }

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::clearF3(const K& k) {
        int b = bucket_for_key(k);

        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << std::endl;
        assert(b != -1);

        buckets_[b].v3.clear();
    }

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::accumulateF1(const K& k, const V1& v) {
        int b = bucket_for_key(k);

        //cout << "accumulate " << k << "\t" << v << endl;
        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << "key: " << k;
        assert(b != -1);

        // lock(this->shard());
        ((IterateKernel<K, V1, V3, E>*)info_.iterkernel)->accumulate(buckets_[b].v1, v);
        ((IterateKernel<K, V1, V3, E>*)info_.iterkernel)->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1);
        // unlock(this->shard());
    }

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::accumulateF2(const K& k, const V2& v) {
        int b = bucket_for_key(k);
        //累积value
        if (b == -1)
            logstream(LOG_FATAL) << "No entry for requested key <" << *((int*) &k) << ">" << std::endl;
        assert(b != -1);
        // lock(this->shard());
        ((IterateKernel<K, V1, V3, E>*)info_.iterkernel)->accumulate(buckets_[b].v2, v);
        // unlock(this->shard());
    }

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::accumulateF3(const K& k, const vector<V3>& v) {

    }

    template <class K, class V1, class V2, class V3, class E>
    void StateTable<K, V1, V2, V3, E>::put(const K& k, const int& edge_count, const V1& v1, const V2& v2, const vector<V3>& v3) {
        int start = bucket_idx(k);
        int b = start;
        bool found = false;
        do {
            if (!buckets_[b].in_use) {
                break;
            }

            if (buckets_[b].k == k) {
                found = true;
                break;
            }

            b = (b + 1) % size_;
        } while (b != start);

        // Inserting a new entry:
        if (!found) {
            if (entries_ > size_) {
                resize((int) (1 + size_ * 2));
                put(k, edge_count, v1, v2, v3);
                ++entries_;
            } else {
                buckets_[b].in_use = 1;
                buckets_[b].k = k;
                buckets_[b].edge_count = edge_count;
                buckets_[b].v1 = v1;
                buckets_[b].v2 = v2;
                buckets_[b].v3 = v3; //vector类型
                ((IterateKernel<K, V1, V3, E>*)info_.iterkernel)->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1);
                ++entries_;
            }
        } else {
            // Replacing an existing entry
            buckets_[b].v1 = v1;
            buckets_[b].v2 = v2;
            buckets_[b].v3 = v3;
            ((IterateKernel<K, V1, V3, E>*)info_.iterkernel)->priority(buckets_[b].priority, buckets_[b].v2, buckets_[b].v1);
        }
    }

}


#endif	/* STATETABLE_H */

