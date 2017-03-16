/* 
 * File:   globaltable.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */

#ifndef THREADTABLE_H
#define	THREADTABLE_H
#include "statetable.h"
#include "table-registry.h"
#include <queue>


namespace dsm {


    extern double termcheck_interval;

    template <class K, class V1, class V2, class V3, class E>
    class GlobalTable {
    public:

        virtual ~GlobalTable() {

            delete statetable;

        }

        virtual void resize(int64_t new_size) {

            statetable->resize(new_size);
            logstream(LOG_INFO) << "buckets size : " << new_size << std::endl;

        }

        int64_t buckets_size() {
            return statetable->size();
        }


    public:
        StateTable<K, V1, V2, V3, E>* statetable;

    };

    template <class K, class V1, class V2, class V3, class E>
    class TypedGlobalTable :
    public GlobalTable<K, V1, V2, V3, E>,
    public TypedTable<K, V1, V2, V3>,
    private boost::noncopyable {
    public:

        TypedGlobalTable(double schedule_portion, Sharder<K>* sharder, TermChecker<K, V2>*termchecker, int& line) :
        schedule_portion(schedule_portion), sharder(sharder), termchecker(termchecker), line(line) {

            m = new Marshal<K>;
            terminated = false;
            binit = false;

        }

        virtual ~TypedGlobalTable() {

            delete m;

        }

        bool initialized() {
            return binit;
        }

        typedef pair<K, V1> KVPair;
        typedef TypedTableIterator<K, V1, V2, V3, E> Iterator;

        void InitStateTable(int num_shards, double schedule_portion,
                Sharder<K>* sharder,
                IterateKernel<K, V1, V3, E>* iterkernel,
                TermChecker<K, V2>* termchecker) {

            this->statetable = create_statetable(schedule_portion, sharder, iterkernel, termchecker);

            binit = true;
        }

        bool isterminated() {
            return terminated;
        }
        int get_shard(const K& k);
        int shard_for_key_str(const StringPiece& k);
        V1 get_localF1(const K& k);
        V2 get_localF2(const K& k);
        V3 get_localF3(const K& k);

        void put(const K &k, const int &edge_count, const V1 &v1, const V2 &v2, const vector<V3> &v3);
        void updateF1(const K &k, const V1 &v);
        void updateF2(const K &k, const V2 &v);
        void updateF3(const K &k, const V3 &v);
        void clearF3(const K &k);
        void accumulateF1(const K &k, const V1 &v); // 2 TypeGloobleTable :TypeTable
        void accumulateF2(const K &k, const V2 &v);
        void accumulateF3(const K &k, const vector<V3> &v);

        // Return the value associated with 'k', possibly blocking for a remote fetch.
        ClutterRecord<K, V1, V2, V3> get(const K &k);
        V1 getF1(const K &k);
        V2 getF2(const K &k);
        vector<V3> getF3(const K &k);
        bool contains(const K &k);
        bool remove(const K &k);

        bool termcheck();

        void TermCheck();

        //external memory mode 
        TypedTableIterator<K, V1, V2, V3, E> * get_iterator(int num_threads, bool inmemmode);

        TableIterator* get_iterator(int num_threads, bool bfilter, bool inmemmode);

        virtual TypedTableIterator<K, V1, V2, V3, E>* get_typed_iterator(int num_threads, bool bfilter, bool inmemmode) {
            return static_cast<TypedTableIterator<K, V1, V2, V3, E>*> (get_iterator(num_threads, bfilter, inmemmode));
        }

        TypedTableIterator<K, V1, V2, V3, E>* get_entirepass_iterator() {
            return (TypedTableIterator<K, V1, V2, V3, E>*) this->statetable->entirepass_iterator();
        }

        Marshal<K> *kmarshal() {
        }

        Marshal<V1> *v1marshal() {
        }

        Marshal<V2> *v2marshal() {
        }

        Marshal<V3> *v3marshal() {
        }

    protected:
        virtual StateTable<K, V1, V2, V3, E>* create_statetable(double schedule_portion,
                Sharder<K>* sharding,
                IterateKernel<K, V1, V3, E>* iterkernel,
                TermChecker<K, V2>* termchecker);
        //int num_threads;
        double schedule_portion;
        Sharder<K>* sharder;
        TermChecker<K, V2> *termchecker;
        double last_termcheck_;
        int &line;
        bool terminated;
        //check init
        bool binit;
        Marshal<K>* m;
    };

    template<class K, class V1, class V2, class V3, class E>
    V1 TypedGlobalTable<K, V1, V2, V3, E>::get_localF1(const K& k) {

        //  int shard = this->get_shard(k);

        return this->statetable->getF1(k);
    }

    template<class K, class V1, class V2, class V3, class E>
    V2 TypedGlobalTable<K, V1, V2, V3, E>::get_localF2(const K& k) {

        //   int shard = this->get_shard(k);

        return this->statetable->getF2(k);
    }

    template<class K, class V1, class V2, class V3, class E>
    V3 TypedGlobalTable<K, V1, V2, V3, E>::get_localF3(const K& k) {

        //  int shard = this->get_shard(k);

        return this->statetable->getF3(k);
    }

    // Store the given key-value pair in this hash. If 'k' has affinity for a
    // remote thread, the application occurs immediately on the local host,
    // and the update is queued for transmission to the owner.

    template<class K, class V1, class V2, class V3, class E>
    void TypedGlobalTable<K, V1, V2, V3, E>::put(const K &k, const int &edge_count, const V1 &v1, const V2 &v2, const vector<V3> &v3) {

        // int shard = this->get_shard(k);
       // cout<<"put过程"<<endl;
        this->statetable->put(k, edge_count, v1, v2, v3);


    }

    template<class K, class V1, class V2, class V3, class E>
    void TypedGlobalTable<K, V1, V2, V3, E>::updateF1(const K &k, const V1 &v) {

        // int shard = this->get_shard(k);

        this->statetable->updateF1(k, v);

        //  logstream(LOG_INFO) << "update F1 shard " << shard << std::endl;

    }

    template<class K, class V1, class V2, class V3, class E>
    void TypedGlobalTable<K, V1, V2, V3, E>::updateF2(const K &k, const V2 &v) {

        // int shard = this->get_shard(k);

        this->statetable->updateF2(k, v);

        logstream(LOG_INFO) << "update F2 shard " << std::endl;

    }

    template<class K, class V1, class V2, class V3, class E>
    void TypedGlobalTable<K, V1, V2, V3, E>::updateF3(const K &k, const V3 &v) {

        // int shard = this->get_shard(k);
        
        this->statetable->updateF3(k, v);

       //logstream(LOG_INFO) << "update F3 shard " << std::endl;
    }

    template<class K, class V1, class V2, class V3, class E>
    void TypedGlobalTable<K, V1, V2, V3, E>::clearF3(const K &k) {

        // int shard = this->get_shard(k);

        this->statetable->clearF3(k);

        logstream(LOG_INFO) << "update F3 shard " << std::endl;
    }

    template<class K, class V1, class V2, class V3, class E>
    void TypedGlobalTable<K, V1, V2, V3, E>::accumulateF1(const K &k, const V1 &v) { //3

        // int shard = this->get_shard(k);

        this->statetable->accumulateF1(k, v); //TypeTable

        //logstream(LOG_INFO) << "accumulate F1 shard " << shard << std::endl;
    }

    template<class K, class V1, class V2, class V3, class E>
    void TypedGlobalTable<K, V1, V2, V3, E>::accumulateF2(const K &k, const V2 &v) { // 1

        //int shard = this->get_shard(k);

        this->statetable->accumulateF2(k, v); // Typetable

        // logstream(LOG_INFO) << "accumulate F2 shard " << shard << std::endl;

    }

    template<class K, class V1, class V2, class V3, class E>
    void TypedGlobalTable<K, V1, V2, V3, E>::accumulateF3(const K &k, const vector<V3> &v) {

        //int shard = this->get_shard(k);

        this->statetable->accumulateF3(k, v); // Typetable

        logstream(LOG_INFO) << "accumulate F3 shard " << std::endl;
    }


    // Return the value associated with 'k', possibly blocking for a remote fetch.

    template<class K, class V1, class V2, class V3, class E>
    ClutterRecord<K, V1, V2, V3> TypedGlobalTable<K, V1, V2, V3, E>::get(const K &k) {

        //int shard = this->get_shard(k);

        return this->statetable->get(k);
    }

    // Return the value associated with 'k', possibly blocking for a remote fetch.

    template<class K, class V1, class V2, class V3, class E>
    V1 TypedGlobalTable<K, V1, V2, V3, E>::getF1(const K &k) {

        // int shard = this->get_shard(k);

        return this->statetable->getF1(k);
    }

    // Return the value associated with 'k', possibly blocking for a remote fetch.

    template<class K, class V1, class V2, class V3, class E>
    V2 TypedGlobalTable<K, V1, V2, V3, E>::getF2(const K &k) {

        // int shard = this->get_shard(k);

        return this->statetable->getF2(k);
    }

    // Return the value associated with 'k', possibly blocking for a remote fetch.

    template<class K, class V1, class V2, class V3, class E>
    vector<V3> TypedGlobalTable<K, V1, V2, V3, E>::getF3(const K &k) {

        //int shard = this->get_shard(k);

        return this->statetable->getF3(k);
    }

    template<class K, class V1, class V2, class V3, class E>
    bool TypedGlobalTable<K, V1, V2, V3, E>::contains(const K &k) {

        //  int shard = this->get_shard(k);

        return this->statetable->contains(k);

    }

    template<class K, class V1, class V2, class V3, class E>
    bool TypedGlobalTable<K, V1, V2, V3, E>::remove(const K &k) {

        //int shard = this->get_shard(k);

        return this->statetable->remove(k);


    }

    template<class K, class V1, class V2, class V3, class E>
    StateTable<K, V1, V2, V3, E>* TypedGlobalTable<K, V1, V2, V3, E>::create_statetable(double schedule_portion,
            Sharder<K>* sharder,
            IterateKernel<K, V1, V3, E>* iterkernel,
            TermChecker<K, V2>* termchecker) {

        return CreateTable<K, V1, V2, V3, E>(schedule_portion, sharder, iterkernel, termchecker);

    }

    template<class K, class V1, class V2, class V3, class E>
    TypedTableIterator<K, V1, V2, V3, E> * TypedGlobalTable<K, V1, V2, V3, E>::get_iterator(int num_threads, bool inmemmode) {

        return (TypedTableIterator<K, V1, V2, V3, E>*)this->statetable->get_iterator(num_threads, false, inmemmode);
    }

    template<class K, class V1, class V2, class V3, class E>
    TableIterator* TypedGlobalTable<K, V1, V2, V3, E>::get_iterator(int num_threads, bool bfilter, bool inmemmode) {
        if (schedule_portion < 1) {
            return (TypedTableIterator<K, V1, V2, V3, E>*)this->statetable->schedule_iterator(num_threads, bfilter, inmemmode);
        } else {
            return (TypedTableIterator<K, V1, V2, V3, E>*)this->statetable->get_iterator(num_threads, bfilter, inmemmode);
        }
    }

    template<class K, class V1, class V2, class V3, class E>
    bool TypedGlobalTable<K, V1, V2, V3, E>::termcheck() {
        Timer cp_timer;
        double startcheck = Now();
        double total_current = 0;
        long total_updates = 0;

        this->statetable->serializeToSnapshot(&total_updates, &total_current);

        bool bterm = ((TermChecker<int, double>*)(termchecker))->terminate(total_current);

        logstream(LOG_INFO) << "Termination check elapsed time:" << cp_timer.elapsed() << " total current " <<
                StringPrintf("%.05f", ((TermChecker<int, double>*)(termchecker))->set_curr()) <<
                " total updates " << total_updates << endl;

        return bterm;


    }

    template<class K, class V1, class V2, class V3, class E>
    void TypedGlobalTable<K, V1, V2, V3, E>::TermCheck() {

        Timer t;
        last_termcheck_ = Now();
        logstream(LOG_INFO) << "Start termCheck " << std::endl;
        logstream(LOG_INFO) << "termcheck_interval :" << termcheck_interval << std::endl;
        while (!terminated) {

            if (Now() - last_termcheck_ > termcheck_interval) {
                bool bterm = termcheck();

                last_termcheck_ = Now();

                if (bterm) {

                    this->statetable->terminate();

                    terminated = true;
                }
                logstream(LOG_INFO) << "terminated is  " << terminated << std::endl;

            }
        }
        logstream(LOG_INFO) << "end termCheck " << t.elapsed() << std::endl;
    }
}

#endif	/* THREADTABLE_H */

