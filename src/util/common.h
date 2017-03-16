/* 
 * File:   common.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#ifndef COMMON_H_
#define COMMON_H_

#include <time.h>
#include <vector>
#include <string>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <google/protobuf/message.h>

#include "hash.h"
#include "static-initializers.h"
#include "stringpiece.h"
#include "timer.h"

#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include <boost/type_traits.hpp>
#include <boost/utility/enable_if.hpp>

using std::map;
using std::vector;
using std::string;
using std::pair;
using std::make_pair;
using std::tr1::unordered_map;
using std::tr1::unordered_set;

namespace dsm {

void Init(int argc, char** argv);

uint64_t get_memory_rss();
uint64_t get_memory_total();

void Sleep(double t);
void DumpProfile();

double get_processor_frequency();

// Log-bucketed histogram.
class Histogram {
public:
  Histogram() : count(0) {}

  void add(double val);
  string summary();

  int bucketForVal(double v);
  double valForBucket(int b);

  int getCount() { return count; }
private:

  int count;
  vector<int> buckets;
  static const double kMinVal;
  static const double kLogBase;
};

class SpinLock {
public:
  SpinLock() : d(0) {}
  void lock() volatile;
  void unlock() volatile;
private:
    //多任务环境下各任务间共享的标志应该加volatile；
  volatile int d;
};

static double rand_double() {
  return double(random()) / RAND_MAX;
}

// Simple wrapper around a string->double map.
struct Stats {
  double& operator[](const string& key) {
    return p_[key];
  }

  string ToString(string prefix) {
    string out;
    for (unordered_map<string, double>::iterator i = p_.begin(); i != p_.end(); ++i) {
      out += StringPrintf("%s -- %s : %.2f\n", prefix.c_str(), i->first.c_str(), i->second);
    }
    return out;
  }

  void Merge(Stats &other) {
    for (unordered_map<string, double>::iterator i = other.p_.begin(); i != other.p_.end(); ++i) {
      p_[i->first] += i->second;
    }
  }
private:
  unordered_map<string, double> p_;
};

struct MarshalBase {};
//排列
template <class T, class Enable = void>
struct Marshal : public MarshalBase {
  virtual void marshal(const T& t, string* out) {//ToString
    //GOOGLE_GLOG_COMPILE_ASSERT(std::tr1::is_pod<T>::value, Invalid_Value_Type);
    out->assign(reinterpret_cast<const char*>(&t), sizeof(t));
  }

  virtual void unmarshal(const StringPiece& s, T *t) {//FromString
    //GOOGLE_GLOG_COMPILE_ASSERT(std::tr1::is_pod<T>::value, Invalid_Value_Type);
      
      //static_cast一般是普通数据类型(如int m=static_cast<int>(3.14));   
    *t = *reinterpret_cast<const T*>(s.data);
  }
};

template <class T>
struct Marshal<T, typename boost::enable_if<boost::is_base_of<string, T> >::type> : public MarshalBase {
  void marshal(const string& t, string *out) { *out = t; }
  void unmarshal(const StringPiece& s, string *t) { t->assign(s.data, s.len); }
};

template <class T>
struct Marshal<T, typename boost::enable_if<boost::is_base_of<google::protobuf::Message, T> >::type> : public MarshalBase {
  void marshal(const google::protobuf::Message& t, string *out) { t.SerializePartialToString(out); }
  void unmarshal(const StringPiece& s, google::protobuf::Message* t) { t->ParseFromArray(s.data, s.len); }
};

template <class T>
string marshal(Marshal<T>* m, const T& t) { string out; m->marshal(t, &out); return out; }

template <class T>
T unmarshal(Marshal<T>* m, const StringPiece& s) { T out; m->unmarshal(s, &out); return out; }

static vector<int> range(int from, int to, int step=1) {
  vector<int> out;
  for (int i = from; i < to; ++i) {
    out.push_back(i);
  }
  return out;
}

static vector<int> range(int to) {
  return range(0, to);
}
}

#define IN(container, item) (std::find(container.begin(), container.end(), item) != container.end())
#define COMPILE_ASSERT(x) extern int __dummy[(int)x]


#endif /* COMMON_H_ */
