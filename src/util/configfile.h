/* 
 * File:   configfile.h
 *  
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */

#ifndef CONFIGFILE_H
#define	CONFIGFILE_H
#include <iostream>
#include <cstdio>
#include <string>
#include <map>
#include <assert.h>

namespace dsm  {
    
    // Code for trimming strings copied from + modified
    // http://stackoverflow.com/questions/479080/trim-is-not-part-of-the-standard-c-c-library
    const std::string whiteSpaces( " \f\n\r\t\v" );
    
    
    static void trimRight( std::string &str,
                          const std::string& trimChars )
    {
        std::string::size_type pos = str.find_last_not_of( trimChars );
        str. erase( pos + 1 );    
    }

    
    static void trimLeft( std::string &str,
                         const std::string& trimChars )
    {
        std::string::size_type pos = str.find_first_not_of( trimChars );
        str.erase( 0, pos );
    }
    
    
    static std::string trim( std::string str)
    {
        std::string trimChars = " \f\n\r\t\v";
        trimRight( str, trimChars );
        trimLeft( str, trimChars );
        return str;
    }
    
    
    // Removes \n from the end of line
    static void _FIXLINE(char * s) {
        int len = (int)strlen(s)-1; 	  
        if(s[len] == '\n') s[len] = 0;
    }
    
    /**
     * Returns a key-value map of a configuration file key-values.
     * If file is not found, fails with an assertion.
     * @param filename filename of the configuration file
     * @param secondary_filename secondary filename if the first version is not found.
     */
    static std::map<std::string, std::string> loadconfig(std::string filename) {
        FILE * f = fopen(filename.c_str(), "r");  
            if (f == NULL) {
                std::cout << "ERROR: Could not read configuration file: " << filename << std::endl;
                std::cout << "Please define environment variable GRAPHCHI_ROOT or run the program from that directory." << std::endl;
            }
            assert(f != NULL);
     
        
        char s[4096];
        std::map<std::string, std::string> conf;
        
        // I like C parsing more than C++, that is why this is such a mess
        while(fgets(s, 4096, f) != NULL) {
            _FIXLINE(s);
            if (s[0] == '#') continue; // Comment
            if (s[0] == '%') continue; // Comment
            
            char delims[] = "=";
            char * t;
            t = strtok(s, delims);
            const char * ckey = t;
            t = strtok(NULL, delims);
            const char * cval = t;
            
            if (ckey != NULL && cval != NULL) {
                std::string key = trim(std::string(ckey));
                std::string val = trim(std::string(cval));
                conf[key] = val;
            }
        }
        
        
        return conf;
    }
    
};


#endif	/* CONFIGFILE_H */

