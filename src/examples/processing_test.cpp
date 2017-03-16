#include <cstdlib>
#include <iostream>
#include "../kernel/preprocessing/conversions.h"
#include "../kernel/io/ioutil.h"
using namespace dsm;

/*
 * 
 */

typedef float EdgeDataType;
#define SHARDSIZE  3039126

/*int main(int argc, char** argv) {


    int nshards = convert_if_notexists<EdgeDataType>("/home/lijj/data/input_graph", "2");
    std::cout << "nshards" << nshards;
    edge_with_value<EdgeDataType> *buf;
    buf = (edge_with_value<EdgeDataType> *)calloc(SHARDSIZE, sizeof (edge_with_value<EdgeDataType>));

    std::string shardname = filename_shard_edata_block("/home/lijj/data/input_graph", 1, 2);

    int f = open(shardname.c_str(), O_RDONLY);
    if (f < 0) {
        logstream(LOG_ERROR) << "Could not open " << shardname << " error: " << strerror(errno) << std::endl;
        logstream(LOG_DEBUG) << "mkdir _blockdir_ ..." << std::endl;
    }
    assert(f >= 0);


    reada( f, buf, SHARDSIZE*sizeof(edge_with_value<EdgeDataType>));
   //  ssize_t a = read(f, buf, SHARDSIZE*sizeof(edge_with_value<EdgeDataType>));
 
    // std::cout<<"a:"<<a<<std::endl;
    for (int i = 0; i < 10; i++) {
        std::cout << " src  " << buf[i].src << " dst " << buf[i].dst << " value  " << buf[i].value << std::endl;
    }
    std::cout << "the last edge" << std::endl;
    std::cout << " src  " << buf[SHARDSIZE-1].src << " dst " << buf[SHARDSIZE-1].dst << " value  " << buf[SHARDSIZE-1].value << std::endl;

    return 0;
}*/
