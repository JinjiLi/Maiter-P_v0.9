/* 
 * File:   static-initializers.cc
 *  
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#include "static-initializers.h"

#include <stdio.h>

using namespace std;

namespace dsm {

typedef Registry<InitHelper>::Map InitMap;
typedef Registry<TestHelper>::Map TestMap;

void RunInitializers() {
//  fprintf(stderr, "Running %zd initializers... \n", helpers()->size());
  for (InitMap::iterator i = Registry<InitHelper>::get_map().begin();
      i != Registry<InitHelper>::get_map().end(); ++i) {
    i->second->Run();
  }
}

void RunTests() {
  fprintf(stderr, "Starting tests...\n");
  int c = 1;
  TestMap& m = Registry<TestHelper>::get_map();
  for (TestMap::iterator i = m.begin(); i != m.end(); ++i) {
    //fprintf(stderr, "Running test %5d/%5d: %s\n", c, m.size(), i->first.c_str());
    i->second->Run();
    ++c;
  }
  fprintf(stderr, "Done.\n");
}


}
