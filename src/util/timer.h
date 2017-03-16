/* 
 * File:   timer.h
 *  
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#ifndef TIMER_H_
#define TIMER_H_
#include <time.h>
namespace dsm {
static uint64_t rdtsc() {
  uint32_t hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return (((uint64_t)hi)<<32) | ((uint64_t)lo);
}

inline double Now() {
  timespec tp;
  clock_gettime(CLOCK_MONOTONIC, &tp);
  return tp.tv_sec + 1e-9 * tp.tv_nsec;
}

class Timer {
public:
  Timer() {
    Reset();
  }

  void Reset() {
    start_time_ = Now();
    start_cycle_ = rdtsc();
  }

  double elapsed() const {
    return Now() - start_time_;
  }

  uint64_t cycles_elapsed() const {
    return rdtsc() - start_cycle_;
  }

  // Rate at which an event occurs.
  double rate(int count) {
    return count / (Now() - start_time_);
  }

  double cycle_rate(int count) {
    return double(cycles_elapsed()) / count;
  }

private:
  double start_time_;
  uint64_t start_cycle_;
};

}

#define EVERY_N(interval, operation)\
{ static int COUNT = 0;\
  if (COUNT++ % interval == 0) {\
    operation;\
  }\
}

#define PERIODIC(interval, operation)\
{ static int64_t last = 0;\
  static int64_t cycles = (int64_t)(interval * get_processor_frequency());\
  static int COUNT = 0; \
  ++COUNT; \
  int64_t now = rdtsc(); \
  if (now - last > cycles) {\
    last = now;\
    operation;\
    COUNT = 0;\
  }\
}



#endif /* TIMER_H_ */
