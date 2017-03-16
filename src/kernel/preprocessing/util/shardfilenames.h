/* 
 * File:   shardfilenames.h
 * 
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#ifndef SHARDFILENAMES_H
#define	SHARDFILENAMES_H

#include <fstream>
#include <fcntl.h>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <vector>
#include <sys/stat.h>
#include <sys/types.h>

#include "../../../util/logger.h"
#include "../../../util/cmdopts.h"

namespace dsm {

#ifdef __GNUC__
#define VARIABLE_IS_NOT_USED __attribute__ ((unused))
#else
#define VARIABLE_IS_NOT_USED
#endif
    // static int VARIABLE_IS_NOT_USED get_option_int(const char *option_name, int default_value);

    static std::string filename_intervals(std::string basefilename, int nshards) {
        std::stringstream ss;
        ss << basefilename;
        ss << "." << nshards << ".intervals";
        return ss.str();
    }

    static std::string dirname_shard_edata_block(std::string basefilename, size_t nshard) {
        std::stringstream ss;
        ss << basefilename;
        ss << "_blockdir_" << nshard;
        return ss.str();
    }

    template <typename EdgeDataType>
    static size_t get_shard_edata_filesize(std::string edata_shardname) {
        size_t fsize;
        std::string fname = edata_shardname + ".size";
        std::ifstream ifs(fname.c_str());
        if (!ifs.good()) {
            logstream(LOG_FATAL) << "Could not load " << fname << ". Preprocessing forgotten?" << std::endl;
            assert(ifs.good());
        }
        ifs >> fsize;
        ifs.close();
        return fsize;
    }

    static std::string filename_shard_edata_block(std::string basefilename, int blockid, size_t nshard) {
        std::stringstream ss;
        ss << dirname_shard_edata_block(basefilename, nshard);
        ss << "/";
        ss << blockid;
        return ss.str();
    }

    template <typename EdgeDataType>
    static std::string filename_shard_edata(std::string basefilename, int p, int nshards) {
        std::stringstream ss;
        ss << basefilename;
        ss << "e" << sizeof (EdgeDataType) << "B.";
        ss << p << "_" << nshards;

        return ss.str();
    }

    static std::string filename_shard_adj(std::string basefilename, int p, int nshards) {
        std::stringstream ss;
        ss << basefilename;
        ss << ".edata_azv.";
        ss << p << "_" << nshards << ".adj";
        return ss.str();
    }


    static bool file_exists(std::string sname);

    static bool file_exists(std::string sname) {
        int tryf = open(sname.c_str(), O_RDONLY);
        if (tryf < 0) {
            return false;
        } else {
            close(tryf);
            return true;
        }
    }

    /**
     * Returns the number of shards if a file has been already
     * sharded or 0 if not found.
     */
    template<typename EdgeDataType>
    static int find_shards(std::string base_filename, std::string shard_string = "auto") {
        int try_shard_num;
        int start_num = 1;
        int last_shard_num = 2400;
        if (shard_string == "auto") {
            start_num = 1;
        } else {
            start_num = atoi(shard_string.c_str());
            last_shard_num = start_num;
        }

        size_t blocksize = 1024 * 1024;
        //  while (blocksize % sizeof(EdgeDataType) != 0) blocksize++;

        for (try_shard_num = start_num; try_shard_num <= last_shard_num; try_shard_num++) {

            int nshards_candidate = try_shard_num;
            bool success = true;

            // Validate all relevant files exists
            for (int p = 0; p < nshards_candidate; p++) {
                std::string sname = filename_shard_edata_block(
                        base_filename, p, nshards_candidate);
                if (!file_exists(sname)) {
                    logstream(LOG_DEBUG) << "Missing directory file: " << sname << std::endl;
                    success = false;
                    break;
                }
            }
            if (!success) {
                continue;
            }

            if (shard_string == "auto") {
                logstream(LOG_INFO) << "Detected number of shards: " << nshards_candidate << std::endl;
                return nshards_candidate;
            } else {
                if (success) {
                    logstream(LOG_INFO) << "Detected number of shards: " << nshards_candidate << std::endl;
                    return nshards_candidate;
                }
            }

        }
        if (try_shard_num > last_shard_num) {
            logstream(LOG_WARNING) << "Could not find shards with nshards = " << start_num << std::endl;
            logstream(LOG_WARNING) << "Please define 'nshards 0' or 'nshards auto' to automatically detect." << std::endl;
        }
        return 0;
    }

    /**
     * Delete the shard files
     */
    template<typename EdgeDataType>
    static void delete_shards(std::string base_filename, int nshards) {

        logstream(LOG_DEBUG) << "Deleting files for " << base_filename << " shards=" << nshards << std::endl;

        for (int blockid = 0; blockid < nshards; blockid++) {
            std::string block_filename = filename_shard_edata_block(base_filename, blockid, nshards);
            if (file_exists(block_filename)) {
                int err = remove(block_filename.c_str());
                if (err != 0) logstream(LOG_ERROR) << "Error removing file " << block_filename
                        << ", " << strerror(errno) << std::endl;
            }
        }

        std::string dirname = dirname_shard_edata_block(base_filename, nshards);
        if (file_exists(dirname)) {
            int err = remove(dirname.c_str());
            if (err != 0) logstream(LOG_ERROR) << "Error removing directory " << dirname
                    << ", " << strerror(errno) << std::endl;

        }

        std::string numv_filename = base_filename + ".numvertices";
        if (file_exists(numv_filename)) {
            int err = remove(numv_filename.c_str());
            if (err != 0) logstream(LOG_ERROR) << "Error removing file " << numv_filename
                    << ", " << strerror(errno) << std::endl;
        }

        std::string intervalfname = filename_intervals(base_filename, nshards);
        if (file_exists(intervalfname)) {
            int err = remove(intervalfname.c_str());
            if (err != 0) logstream(LOG_ERROR) << "Error removing file " << intervalfname
                    << ", " << strerror(errno) << std::endl;

        }

    }




    static VARIABLE_IS_NOT_USED size_t get_num_vertices(std::string basefilename);

    static VARIABLE_IS_NOT_USED size_t get_num_vertices(std::string basefilename) {
        std::string numv_filename = basefilename + ".numvertices";
        std::ifstream vfileF(numv_filename.c_str());
        if (!vfileF.good()) {
            logstream(LOG_ERROR) << "Could not find file " << numv_filename << std::endl;
            logstream(LOG_ERROR) << "Maybe you have old shards - please recreate." << std::endl;
            assert(false);
        }
        size_t n;
        vfileF >> n;
        vfileF.close();
        return n;
    }

    /**
     * Checks if original file has more recent modification date
     * than the shards. If it has, deletes the shards and returns false.
     * Otherwise return true.
     */
    template <typename EdgeDataType>
    bool check_origfile_modification_earlier(std::string basefilename, int nshards) {
        /* Compare last modified dates of the original graph and the shards */
        if (file_exists(basefilename) && get_config_option_int("DISABLE-MODTIME-CHECK", 0) == 0) {
            // 定义函数：int stat(const char * file_name, struct stat *buf);
            //函数说明：stat()用来将参数file_name 所指的文件状态, 复制到参数buf 所指的结构中。
            struct stat origstat, shardstat;
            int err1 = stat(basefilename.c_str(), &origstat);

            std::string adjfname = filename_shard_edata_block(
                    basefilename, 0, nshards);
            int err2 = stat(adjfname.c_str(), &shardstat);

            if (err1 != 0 || err2 != 0) {
                logstream(LOG_ERROR) << "Error when checking file modification times:  " << strerror(errno) << std::endl;
                return nshards;
            }

            if (origstat.st_mtime > shardstat.st_mtime) {
                logstream(LOG_INFO) << "The input graph modification date was newer than of the shards." << std::endl;
                logstream(LOG_INFO) << "Going to delete old shards and recreate new ones. To disable " << std::endl;
                logstream(LOG_INFO) << "functionality, specify --disable-modtime-check=1" << std::endl;

                // Delete shards
                delete_shards<EdgeDataType>(basefilename, nshards);

                return false;
            } else {
                return true;
            }


        }
        return true;
    }

}


#endif	/* SHARDFILENAMES_H */

