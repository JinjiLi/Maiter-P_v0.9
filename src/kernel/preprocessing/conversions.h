/* 
 * File:   conversions.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#ifndef CONVERSIONS_H
#define	CONVERSIONS_H
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <string.h>
#include <sstream>
#include "../../util/logger.h"
#include "util/shardfilenames.h"
#include "boost/lexical_cast.hpp"
#include "sharder.h"
#include <string>
/**
 * GNU COMPILER HACK TO PREVENT WARNINGS "Unused variable", if
 * the particular app being compiled does not use a function.
 */
#ifdef __GNUC__
#define VARIABLE_IS_NOT_USED __attribute__ ((unused))
#else
#define VARIABLE_IS_NOT_USED
#endif

namespace dsm {
    extern std::map<vid_t, vid_t> src_outnumedges;
    /* Simple string to number parsers */
    static void VARIABLE_IS_NOT_USED parse(int &x, const char * s);
    static void VARIABLE_IS_NOT_USED parse(unsigned int &x, const char * s);
    static void VARIABLE_IS_NOT_USED parse(float &x, const char * s);
    static void VARIABLE_IS_NOT_USED parse(long &x, const char * s);
    static void VARIABLE_IS_NOT_USED parse(char &x, const char * s);
    static void VARIABLE_IS_NOT_USED parse(bool &x, const char * s);
    static void VARIABLE_IS_NOT_USED parse(double &x, const char * s);
    static void VARIABLE_IS_NOT_USED parse(short &x, const char * s);
    static void FIXLINE(char * s);

    static void parse(int &x, const char * s) {
        x = atoi(s);
    }

    static void parse(unsigned int &x, const char * s) {
        x = (unsigned int) strtoul(s, NULL, 10);
    }

    static void parse(float &x, const char * s) {
        x = (float) atof(s);
    }

    static void parse(long &x, const char * s) {
        x = atol(s);
    }

    static void parse(char &x, const char * s) {
        x = s[0];
    }

    static void parse(bool &x, const char * s) {
        x = atoi(s) == 1;
    }

    static void parse(double &x, const char * s) {
        x = atof(s);
    }

    static void parse(short &x, const char * s) {
        x = (short) atoi(s);
    }

    // Removes \n from the end of line

    inline void FIXLINE(char * s) {
        int len = (int) strlen(s) - 1;
        if (s[len] == '\n') s[len] = 0;
    }


    /**
     * Converts a graph from adjacency list format. Edge values are not supported,
     * and each edge gets the default value for the type. Self-edges are ignored.
     */
    //转换邻接表

    template <typename EdgeDataType>
    void convert_adjlist(std::string inputfile, sharder<EdgeDataType> &sharderobj) {

        std::ifstream inFile;
        inFile.open(inputfile.c_str());
        if (!inFile) {
            logstream(LOG_FATAL) << "Could not load :" << inputfile << " error: " << strerror(errno) << std::endl;
        }

        logstream(LOG_INFO) << "Reading in adjacency list format!" << std::endl;

        std::string line;
        size_t linenum = 0;
        size_t bytesread = 0;
        size_t lastlog = 0;
        //char delims[] = " \t";
        while (std::getline(inFile, line)) {
            linenum++;
            if (bytesread - lastlog >= 500000000) {
                logstream(LOG_DEBUG) << "Read " << linenum << " lines, " << bytesread / 1024 / 1024. << " MB" << std::endl;
                lastlog = bytesread;
            }
            bytesread += line.size();

            if (line[0] == '#') continue; // Comment
            if (line[0] == '%') continue; // Comment

            int pos = line.find("\t");
            vid_t from = atoi(line.substr(0, pos).c_str());
            // logstream(LOG_ERROR) <<"from"<<from<<std::endl;

            std::string links = line.substr(pos + 1);
            //  logstream(LOG_ERROR) <<"links"<<links<<std::endl;

            pos = links.find_first_of(" ");
            vid_t num = atoi(links.substr(0, pos).c_str());
            //  logstream(LOG_ERROR) <<"num"<<num<<std::endl;
            //sleep(2);
            links = links.substr(pos + 1);

            if (*(links.end() - 1) != ' ') {
                links = links + " ";
            }
            //   logstream(LOG_ERROR) <<"links"<<links<<std::endl;

            int spacepos = 0;
            vid_t i = 0;

            while ((spacepos = links.find_first_of(" ")) != links.npos) {
                std::string element(links.substr(0, spacepos));
                int cut = 0;
                if ((cut = element.find_first_of(",")) != element.npos) {
                    vid_t to = atoi(element.substr(0, cut).c_str());
                    EdgeDataType weight = boost::lexical_cast<EdgeDataType>(element.substr(cut + 1));
                    sharderobj.preprocessing_add_edge(from, to, weight);
                    //i++;
                } else {
                    vid_t to = atoi(element.c_str());
                    // logstream(LOG_ERROR) <<"to"<<to<<std::endl;
                    sharderobj.preprocessing_add_edge(from, to);
                    //i++;
                }
                i++;
                links = links.substr(spacepos + 1);

            }

            if (num != i) {
                logstream(LOG_ERROR) << "Mismatch when reading adjacency list: " << num << " != " << i << " line: " << line
                        << " on line: " << linenum << std::endl;
                continue;
            }
            
            src_outnumedges.insert(std::map<vid_t,vid_t>::value_type(from,num));
        }
        logstream(LOG_INFO) << "linenum" << linenum << std::endl;
        inFile.close();

    }

    /**
     * Converts a graph input to shards. Preprocessing has several steps,
     * see sharder.hpp for more information.
     */
    template <typename EdgeDataType>
    int convert(std::string basefilename, std::string nshards_string) {
        sharder<EdgeDataType> sharderobj(basefilename);

        /* Start preprocessing */
        sharderobj.start_preprocessing();


        convert_adjlist<EdgeDataType>(basefilename, sharderobj);

        /* Finish preprocessing */
        sharderobj.end_preprocessing();

        int nshards = sharderobj.execute_sharding(nshards_string);

        logstream(LOG_INFO) << "Successfully finished sharding for " << basefilename << std::endl;
        logstream(LOG_INFO) << "Created " << nshards << " shards." << std::endl;
        return nshards;
    }

    template <typename EdgeDataType>
    int convert_if_notexists(std::string basefilename, std::string nshards_string, bool &didexist) {
        int nshards;

        /* Check if input file is already sharded */
        if ((nshards = find_shards<EdgeDataType>(basefilename, nshards_string))) {
            logstream(LOG_INFO) << "Found preprocessed files for " << basefilename << ", num shards=" << nshards << std::endl;
            didexist = true;
            if (check_origfile_modification_earlier<EdgeDataType>(basefilename, nshards)) {
                return nshards;
            }

        }
        didexist = false;

        logstream(LOG_INFO) << "Did not find preprocessed shards for " << basefilename << std::endl;

        logstream(LOG_INFO) << "(Edge-value size: " << sizeof (EdgeDataType) << ")" << std::endl;
        logstream(LOG_INFO) << "Will try create them now..." << std::endl;
        nshards = convert<EdgeDataType>(basefilename, nshards_string);
        return nshards;
    }

    template <typename EdgeDataType>
    int convert_if_notexists(std::string basefilename, std::string nshards_string) {
        bool b;
        return convert_if_notexists<EdgeDataType>(basefilename, nshards_string, b);
    }


} // end namespace



#endif	/* CONVERSIONS_H */

