CC:=g++
CFLAGS1=-Isrc/util
CFLAGS1+=-c
CFLAGS2:=-I./src/kernel 
CFLAGS2+=-I./src/util
CFLAGS2+=-I./src/kernel/preprocessing
CFLAGS2+=-I./src/kernel/preprocessing/util
CFLAGS2+=-I./src/kernel/io
CFLAGS2+=-lpthread
CFLAGS2+=-c
TARGET1:=bin/pagerank
DEPEND:=obj/static-initializers.o
DEPEND+=obj/stringpiece.o
DEPEND+=obj/common.o
DEPEND+=obj/var-declare.o
DEPEND+=obj/vertexlock-manager.o
DEPEND1:=obj/pagerank.o


$(TARGET1):$(DEPEND) $(DEPEND1)
	$(CC) -o $@  $^ -lpthread 

obj/%.o:src/util/%.cpp
	$(CC) -o $@ $(CFLAGS1) $^

obj/%.o:src/kernel/%.cpp
	$(CC) -o $@ $(CFLAGS1) $^

$(DEPEND1):src/examples/pagerank.cpp
	$(CC) -o $@ $(CFLAGS2) $^ 




all:bin/pagerank  

.PHONY:clean

clean:
	rm -rf obj/*  bin/*
			  
