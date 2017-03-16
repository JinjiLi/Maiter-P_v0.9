/* 
 * File:   pagerank.cpp
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#include <iostream>
#include <fstream>
#include "../kernel/kernel.h"
#include <cstdlib>
#include "../kernel/preprocessing/conversions.h"
using namespace dsm;

//DECLARE_string(graph_dir);
typedef int K;
typedef float V;
typedef int D;
typedef float EdgeDataType;

struct In_Memory_PagerankIterateKernel : public IterateKernel<K, V, D, EdgeDataType> {
    float zero;

    In_Memory_PagerankIterateKernel() : zero(0) {
    }

    //对于外存

    void read_data(string& line, K& k, int &edge_count, vector<D>& data) {
        string linestr(line);
        int pos = linestr.find("\t"); //找到制表符的位置
        if (pos == -1) return;

        int source = atoi(linestr.substr(0, pos).c_str()); //字符串转换成数值  源点

        vector<D> linkvec;
        string links = linestr.substr(pos + 1);
        pos = links.find_first_of(" ");
        int num = atoi(links.substr(0, pos).c_str());
        links = links.substr(pos + 1);
        //cout<<"links:"<<links<<endl;
        if (*(links.end() - 1) != ' ') {
            links = links + " ";
        }
        int spacepos = 0;
        while ((spacepos = links.find_first_of(" ")) != links.npos) {
            int to;
            if (spacepos > 0) {
                to = atoi(links.substr(0, spacepos).c_str());
                //cout<<"to:"<<to<<endl;
            }
            links = links.substr(spacepos + 1);
            linkvec.push_back(to);
        }

        k = source;
        edge_count = num;
        data = linkvec;
    }

    void init_c(const K& k, V& delta, vector<D>& data) {

        delta = 0.2;
    }

    void init_v(const K& k, V& v, vector<D>& data) {
        v = 0;
    }

    void accumulate(V& a, const V& b) {
        a = a + b;
    }

    void priority(V& pri, const V& value, const V& delta) {
        pri = delta;
    }

    void g_func(const K& k, const int& edge_count, const V& delta, const V&value, const vector<D>& data, vector<pair<K, V> >* output) {
        int size = edge_count;
        V outv = delta * 0.8 / size;
        //cout << "size " << size << endl;
        for (vector<K>::const_iterator it = data.begin(); it != data.end(); it++) {
            int target = *it;
            output->push_back(make_pair(target, outv));
            //cout << k<<"  "<<target<< "   "<<outv<<endl;

        }
    }

    const V& default_v() const {
        return zero;
    }
};

struct External_Memory_PagerankIterateKernel : public IterateKernel<K, V, D, EdgeDataType > {
    float zero;

    External_Memory_PagerankIterateKernel() : zero(0) {
    }

    //对于外存　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　 //vector中要挂的结构

    void read_eachedge(edge_with_value<EdgeDataType> *edge, K& k, D& data) {


        k = (K) edge->src;
       // cout << "k:" << k << endl;
        data = (D) edge->dst;
        //cout << "data:" << data << endl;

    }

    void init_c(const K& k, V& delta, vector<D>& data) {
        //  cout << "初始化＿Ｃ" << endl;
        delta = 0.2;
    }

    void init_v(const K& k, V& v, vector<D>& data) {
        // cout << "初始化＿Ｖ" << endl;
        v = 0;
    }

    void accumulate(V& a, const V& b) {
        a = a + b;
    }

    void priority(V& pri, const V& value, const V& delta) {
        pri = delta;
    }

    void g_func(const K& k, const int& edge_count, const V& delta, const V&value, const vector<D>& data, vector<pair<K, V> >* output) {
        int size = edge_count;
        V outv = delta * 0.8 / size;
        //cout << "size " << size << endl;
        for (vector<D>::const_iterator it = data.begin(); it != data.end(); it++) {
            D target = *it;
            output->push_back(make_pair(target, outv));
            //cout << k<<"  "<<target<< "   "<<outv<<endl;

        }
    }

    const V& default_v() const {
        return zero;
    }
};

static int Pagerank() {

    maiter_init(); //configuration
    string input = get_config_option_string("INPUT", "input/pagerank");
    string filename = StringPrintf("%s/input_graph", input.c_str());
    int nshards = convert_if_notexists<EdgeDataType>(filename, get_config_option_string("NSHARDS", "auto"));

    bool inmemmode = 2 * (max_vertexid + 1) * sizeof (K) + sumedges * sizeof (D) +
            2 * (max_vertexid + 1) * sizeof (V)+(max_vertexid + 1) * sizeof (int)
            < (size_t) get_config_option_int("MENBUDGET_MB", 1024) * 1024L * 1024L / 3 * 4;
    if (inmemmode && nshards == 1) {
        logstream(LOG_INFO) << "select mode is  " << "inmemory mode" << std::endl;
    } else {
        logstream(LOG_INFO) << "select mode is  " << "external mode" << std::endl;
        inmemmode = false;
    }
    MaiterKernel<K, V, D, EdgeDataType >* kernel = NULL;
    if (inmemmode && nshards == 1) {
        kernel = new MaiterKernel<K, V, D, EdgeDataType >(
                new Sharding::Mod_int,
                new In_Memory_PagerankIterateKernel,
                new TermCheckers<K, V>::Diff, nshards, inmemmode);
    } else {
        kernel = new MaiterKernel<K, V, D, EdgeDataType >(
                new Sharding::Mod_int,
                new External_Memory_PagerankIterateKernel,
                new TermCheckers<K, V>::Diff, nshards, inmemmode);
    }

    RunMaiterKernel<K, V, D, EdgeDataType > * runkernel = new RunMaiterKernel<K, V, D, EdgeDataType>(kernel);

    runkernel->runMaiter();


    delete runkernel;
    return 0;
 
}

/*
 * 
 */
int main(int argc, char** argv) {

    Pagerank();
    return 0;
}