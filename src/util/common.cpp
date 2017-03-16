/* 
 * File:   common.cc
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#include "common.h"
#include "static-initializers.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <execinfo.h>
#include <fcntl.h>

#include <math.h>

#include <asm/msr.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <lzo/lzo1x.h>


#ifdef CPUPROF
#include <google/profiler.h>
DEFINE_bool(cpu_profile, false, "");
#endif

#ifdef HEAPPROF
#include <google/heap-profiler.h>
#include <google/malloc_extension.h>
#endif



namespace dsm {

const double Histogram::kMinVal = 1e-9;
const double Histogram::kLogBase = 1.1;


int Histogram::bucketForVal(double v) {
  if (v < kMinVal) { return 0; }

  v /= kMinVal;
  v += kLogBase;

  return 1 + static_cast<int>(log(v) / log(kLogBase));
}

double Histogram::valForBucket(int b) {
  if (b == 0) { return 0; }
  return exp(log(kLogBase) * (b - 1)) * kMinVal;
}

void Histogram::add(double val) {
  int b = bucketForVal(val);
//  LOG_EVERY_N(INFO, 1000) << "Adding... " << val << " : " << b;
  if (buckets.size() <= b) { buckets.resize(b + 1); }
  ++buckets[b];
  ++count;
}

void DumpProfile() {
#ifdef CPUPROF
  ProfilerFlush();
#endif
}

string Histogram::summary() {
  string out;
  int total = 0;
  for (int i = 0; i < buckets.size(); ++i) { total += buckets[i]; }
  string hashes = string(100, '#');

  for (int i = 0; i < buckets.size(); ++i) {
    if (buckets[i] == 0) { continue; }
    out += StringPrintf("%-20.3g %6d %.*s\n", valForBucket(i), buckets[i], buckets[i] * 80 / total, hashes.c_str());
  }
  return out;
}

uint64_t get_memory_total() {
  uint64_t m = -1;
  FILE* procinfo = fopen(StringPrintf("/proc/meminfo", getpid()).c_str(), "r");
  while (fscanf(procinfo, "MemTotal: %ld kB", &m) != 1) {
    if (fgetc(procinfo) == EOF) { break; }
  }
  fclose(procinfo);

  return m * 1024;
}

uint64_t get_memory_rss() {
  uint64_t m = -1;
  FILE* procinfo = fopen(StringPrintf("/proc/%d/status", getpid()).c_str(), "r");
  while (fscanf(procinfo, "VmRSS: %ld kB", &m) != 1) {
    if (fgetc(procinfo) == EOF) { break; }
  }
  fclose(procinfo);

  return m * 1024;
}

double get_processor_frequency() {
  double freq;
  int a, b;
  FILE* procinfo = fopen("/proc/cpuinfo", "r");
  while (fscanf(procinfo, "cpu MHz : %d.%d", &a, &b) != 2) {
    fgetc(procinfo);
  }

  freq = a * 1e6 + b * 1e-4;
  fclose(procinfo);
  return freq;
}

void Sleep(double t) {
  timespec req;
  req.tv_sec = (int)t;
  req.tv_nsec = (int64_t)(1e9 * (t - (int64_t)t));
  nanosleep(&req, NULL);
}

void SpinLock::lock() volatile {
  while (!__sync_bool_compare_and_swap(&d, 0, 1));
}

void SpinLock::unlock() volatile {
  d = 0;
}



}
