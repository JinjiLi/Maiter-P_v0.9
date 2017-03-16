/* 
 * File:   table.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */

#ifndef TABLE_H
#define	TABLE_H
#define FETCH_NUM 2048

#include "../util/common.h"
#include <boost/lexical_cast.hpp>
#include <assert.h>
#include "../util/logger.h"
#include <pthread.h>
#include "preprocessing/sharder.h"
namespace dsm {

    extern double termcheck_threshold;

    struct TableBase;
    struct Table;

    struct SharderBase {
    };

    struct IterateKernelBase {
    };

    struct TermCheckerBase {
    };
    // This interface is used by global tables to communicate with the outside

    template <class K>
    struct Sharder : public SharderBase {
        virtual int operator()(const K& k, int shards) = 0;
    };

    template <class K, class V, class D, class E>
    struct IterateKernel : public IterateKernelBase {

        virtual void read_data(string& line, K& k, int& edge_count, vector<D>& data) {
        }

        virtual void read_eachedge(edge_with_value<E> *edge, K& k, D& data) {

        }
        virtual void init_c(const K& k, V& delta, vector<D>& data) = 0;
        virtual const V & default_v() const = 0;
        virtual void init_v(const K& k, V& v, vector<D>& data) = 0;
        virtual void accumulate(V& a, const V & b) = 0;

        virtual void process_delta_v(const K& k, V& dalta, V& value, vector<D>& data) {
        }
        virtual void priority(V& pri, const V& value, const V & delta) = 0;
        virtual void g_func(const K& k, const int& edge_count, const V& delta, const V& value, const vector<D>& data, vector<pair<K, V> >* output) = 0;
    };

    template <class K, class V>
    struct LocalTableIterator {
        virtual const K& key() = 0;
        virtual V& value2() = 0;
        virtual bool done() = 0;
        virtual bool Next() = 0;
        virtual V defaultV() = 0;
    };
    class TableIterator;

    struct TableIterator {
        virtual void key_str(string *out) = 0;
        virtual void value1_str(string *out) = 0;
        virtual void value2_str(string *out) = 0;
        virtual bool done() = 0;
        virtual bool Next() = 0;
    };

    template <class K, class V>
    struct TermChecker : public TermCheckerBase {
        virtual double set_curr() = 0;
        virtual double estimate_prog(LocalTableIterator<K, V>* table_itr) = 0;
        virtual bool terminate(double total_current) = 0;
    };

    template <class K, class V>
    struct TermCheckers {

        struct Diff : public TermChecker<K, V> {
            double last;
            double curr;

            Diff() {
                last = -std::numeric_limits<double>::max();
                curr = 0;
            }

            double set_curr() {
                return curr;
            }

            double estimate_prog(LocalTableIterator<K, V>* statetable) {
                double partial_curr = 0;
                V defaultv = statetable->defaultV();
                while (!statetable->done()) {
                    bool cont = statetable->Next();
                    if (!cont) break;
                    if (statetable->value2() != defaultv) {
                        partial_curr += static_cast<double> (statetable->value2());
                    }
                }
                return partial_curr;
            }

            bool terminate(double total_current) {
                curr = total_current;
                logstream(LOG_INFO) << "terminate check : last progress " << last << " current progress " << curr << " difference :" << abs(curr - last) << std::endl;
                if (abs(curr - last) <= termcheck_threshold) {
                    return true;
                } else {
                    last = curr;
                    return false;
                }
            }
        };
    };

    // Commonly used accumulation and sharding operators.

    struct Sharding {

        struct String : public Sharder<string> {

            int operator()(const string& k, int shards) {
                return StringPiece(k).hash() % shards;
            }
        };

        struct Mod_str : public Sharder<string> { //only for simrank

            int operator()(const string& k, int shards) {
                string key = k;
                int pos = key.find("_");
                key = key.substr(0, pos);
                //进行数值转换
                pos = boost::lexical_cast<int>(key);
                return (pos % shards);
            }
        };

        struct Mod_int : public Sharder<int> {

            int operator()(const int& key, int shards) {
                return key % shards;
            }
        };

        struct Mod_vid : public Sharder<vid_t> {

            int operator()(const vid_t& key, int shards) {
                return key % shards;
            }
        };

        struct UintMod : public Sharder<uint32_t> {

            int operator()(const uint32_t& key, int shards) {
                return key % shards;
            }
        };
    };

    struct TableFactory {
        virtual TableBase* New() = 0;
    };

    struct TableDescriptor {
    public:

        TableDescriptor() {
            Reset();
        }

        void Reset() {
            partition_factory = NULL;
            key_marshal = value1_marshal = value2_marshal = value3_marshal = NULL;
            sharder = NULL;
            iterkernel = NULL;
            termchecker = NULL;
        }
        // int num_shards;

        // For local tables, the shard of the global table they represent.
        // int shard;
        int default_shard_size;
        double schedule_portion;


        SharderBase*sharder;
        IterateKernelBase*iterkernel;
        TermCheckerBase*termchecker;

        MarshalBase *key_marshal;
        MarshalBase *value1_marshal;
        MarshalBase *value2_marshal;
        MarshalBase *value3_marshal;
        // For global tables, factory for constructing new statetable.
        TableFactory *partition_factory;
    };

    struct Table {
        //table information
        virtual const TableDescriptor& info() const = 0;
        //shard
        // virtual int shard() const = 0;
        //num_shard
        // virtual int num_shards() const = 0;

    };


    // Methods common to both global and local table views.

    class TableBase : public Table {
    public:
        typedef TableIterator Iterator;

        virtual void Init(const TableDescriptor* info) {
            info_ = *info;
            terminated_ = false;
            assert(info_.key_marshal != NULL);
            assert(info_.value1_marshal != NULL);
            assert(info_.value2_marshal != NULL);
            assert(info_.value3_marshal != NULL);
        }

        const TableDescriptor& info() const {
            return info_;
        }

        /*   int shard() const {
               return info().shard;
           }

           int num_shards() const {
               return info().num_shards;
           }
         */
        void terminate() {
            terminated_ = true;
        }

    protected:
        TableDescriptor info_;
        bool terminated_;
    };

    //id  value  delta edgevector

    template <class K, class V1, class V2, class V3>
    struct ClutterRecord {
        K k;
        V1 v1;
        V2 v2;
        vector<V3> v3;

        ClutterRecord()
        : k(), v1(), v2(), v3() {
        }

        ClutterRecord(const K& __a, const V1& __b, const V2& __c, const vector<V3>& __d)
        : k(__a), v1(__b), v2(__c), v3(__d) {
        }

        template<class K1, class U1, class U2, class U3>
        ClutterRecord(const ClutterRecord<K1, U1, U2, U3>& __p)
        : k(__p.k),
        v1(__p.v1),
        v2(__p.v2),
        v3(__p.v3) {
        }

        ostream& operator<<(ostream& out) {
            return out << k << "\t" << v1 << "|" << v2 << "|" << v3;
        }
    };

    struct UntypedTable {
        virtual bool contains_str(const StringPiece& k) = 0;
        virtual string get_str(const StringPiece &k) = 0;
        virtual void update_str(const StringPiece &k, const StringPiece &v1, const StringPiece &v2, const StringPiece &v3) = 0;
    };

    // Key/value typed interface.

    template <class K, class V1, class V2, class V3>
    class TypedTable : virtual public UntypedTable {
    public:
        virtual bool contains(const K &k) = 0;
        virtual V1 getF1(const K &k) = 0;
        virtual V2 getF2(const K &k) = 0;
        virtual vector<V3> getF3(const K &k) = 0;
        virtual ClutterRecord<K, V1, V2, V3> get(const K &k) = 0;
        virtual void put(const K &k, const int &edge_count, const V1 &v1, const V2 &v2, const vector<V3> &v3) = 0;
        virtual void updateF1(const K &k, const V1 &v) = 0;
        virtual void updateF2(const K &k, const V2 &v) = 0;
        virtual void updateF3(const K &k, const V3 &v) = 0;
        virtual void accumulateF1(const K &k, const V1 &v) = 0; //4 TypeTable
        virtual void accumulateF2(const K &k, const V2 &v) = 0;
        virtual void accumulateF3(const K &k, const vector<V3> &v) = 0;
        virtual bool remove(const K &k) = 0;

        // Default specialization for untyped methods

        virtual bool contains_str(const StringPiece& s) {
            K k;
            kmarshal()->unmarshal(s, &k);
            return contains(k);
        }

        virtual string get_str(const StringPiece &s) {
            K k;
            string f1, f2, f3;

            kmarshal()->unmarshal(s, &k);
            v1marshal()->marshal(getF1(k), &f1);
            v2marshal()->marshal(getF2(k), &f2);
            return f1 + f2 + f3;
        }

        virtual void update_str(const StringPiece& kstr, const StringPiece &vstr1, const StringPiece &vstr2, const StringPiece &vstr3) {
            K k;
            int edge_count;
            V1 v1;
            V2 v2;
            vector<V3> v3;
            kmarshal()->unmarshal(kstr, &k);
            v1marshal()->unmarshal(vstr1, &v1);
            v2marshal()->unmarshal(vstr2, &v2);
            put(k, edge_count, v1, v2, v3);
        }

    protected:
        virtual Marshal<K> *kmarshal() = 0;
        virtual Marshal<V1> *v1marshal() = 0;
        virtual Marshal<V2> *v2marshal() = 0;
    };

    template <class K, class V1, class V2, class V3, class E>
    struct TypedTableIterator : public TableIterator {

        virtual ~TypedTableIterator() {
        }
        virtual const K& key(int64_t pos) = 0;
        virtual const int& edge_count(int64_t pos) = 0;
        virtual V1& value1(int64_t pos) = 0;
        virtual V2& value2(int64_t pos) = 0;
        virtual vector<V3>& value3(int64_t pos) = 0;
        virtual bool is_in_use(int64_t pos) = 0;
        virtual const K& key() = 0;
        virtual const int& edge_count() = 0;
        virtual V1& value1() = 0;
        virtual V2& value2() = 0;
        virtual vector<V3>& value3() = 0;


        virtual int64_t thread_begin(pthread_t thread_id) = 0;

        virtual int64_t thread_end(pthread_t thread_id) = 0;

        virtual vid_t thread_begin_pos(pthread_t _thread_id) = 0;

        virtual int64_t buckets_size() = 0;

        virtual int64_t vector_size(int64_t pos) = 0;

        virtual void vector_clear(int64_t pos) = 0;

        virtual void key_str(string * out) {
            kmarshal()->marshal(key(), out);
        }

        virtual void value1_str(string * out) {
            v1marshal()->marshal(value1(), out);
        }

        virtual void value2_str(string * out) {
            v2marshal()->marshal(value2(), out);
        }



    protected:

        virtual Marshal<K> *kmarshal() {
            static Marshal<K> m;
            return &m;
        }

        virtual Marshal<V1> *v1marshal() {
            static Marshal<V1> m;
            return &m;
        }

        virtual Marshal<V2> *v2marshal() {
            static Marshal<V2> m;
            return &m;
        }


    };

    class Snapshottable {
    public:
        virtual void serializeToSnapshot(long* updates, double* totalF2) = 0;
    };


}


#endif	/* TABLE_H */

