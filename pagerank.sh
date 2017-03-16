#g++ -o pagerank ./src/examples/pagerank.cpp ./src/util/static-initializers.cpp ./src/util/stringpiece.cpp ./src/util/common.cpp    ./src/kernel/var-declare.cpp ./src/kernel/vertexlock-manager.cpp  -I./src/kernel -I./src/kernel/io -I./src/kernel/preprocessing   -I./src/kernel/preprocessing/util  -I./src/util/ -lpthread

./bin/pagerank
