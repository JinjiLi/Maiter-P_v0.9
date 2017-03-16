/* 
 * File:   cmdopts.h
* 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */

#ifndef CMDOPTS_H
#define	CMDOPTS_H

#include <string>
#include <iostream>
#include <stdint.h>
#include <fstream>
#include <fcntl.h>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <vector>
#include <sys/stat.h>
#include "configfile.h"

namespace dsm{ 
    
    /** GNU COMPILER HACK TO PREVENT IT FOR COMPILING METHODS WHICH ARE NOT USED IN
        THE PARTICULAR APP BEING BUILT */
#ifdef __GNUC__
#define VARIABLE_IS_NOT_USED __attribute__ ((unused))
#else
#define VARIABLE_IS_NOT_USED
#endif
    
    
    //static int _argc;
    //static char **_argv;
    static std::map<std::string, std::string> conf;
    
    
    static void VARIABLE_IS_NOT_USED set_conf(std::string key, std::string value) {
        conf[key] = value;
    }
    
    // Config file
    static std::string VARIABLE_IS_NOT_USED get_config_option_string(const char *option_name) {
        if (conf.find(option_name) != conf.end()) {
            return conf[option_name];
        } else {
            std::cout << "ERROR: could not find option " << option_name << " from config.";
            assert(false);
        }
    }
    
    static  std::string VARIABLE_IS_NOT_USED get_config_option_string(const char *option_name,
                                                 std::string default_value) {
        if (conf.find(option_name) != conf.end()) {
            return conf[option_name];
        } else {
            return default_value;
        }
        
    }
    static int VARIABLE_IS_NOT_USED get_config_option_int(const char *option_name, int default_value) {
        if (conf.find(option_name) != conf.end()) {
            return atoi(conf[option_name].c_str());
        } else {
            return default_value;
        }
    }
    
    static int VARIABLE_IS_NOT_USED get_config_option_int(const char *option_name) {
        if (conf.find(option_name) != conf.end()) {
            return atoi(conf[option_name].c_str());
        } else {
            std::cout << "ERROR: could not find option " << option_name << " from config.";
            assert(false);
        }
    }
    
    static uint64_t VARIABLE_IS_NOT_USED get_config_option_long(const char *option_name, uint64_t default_value) {
        if (conf.find(option_name) != conf.end()) {
            return atol(conf[option_name].c_str());
        } else {
            return default_value;
        }
    }
    static double VARIABLE_IS_NOT_USED get_config_option_double(const char *option_name, double default_value) {
        if (conf.find(option_name) != conf.end()) {
            return atof(conf[option_name].c_str());
        } else {
            return default_value;
        }
    }
    static std::string filename_config()
    {
        return "conf/maiter-p_v0.0.cnf";
    }
 
    
    static void maiter_init();
    static void maiter_init()
    {
         conf = loadconfig(filename_config());
        // return conf;
    }
    

} // End namespace


#endif	/* CMDOPTS_H */

