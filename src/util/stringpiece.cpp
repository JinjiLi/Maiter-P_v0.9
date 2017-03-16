/* 
 * File:   stringpiece.cc
 *  
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#include "stringpiece.h"
#include "hash.h"
#include "static-initializers.h"
#include <assert.h>

#include <stdarg.h>
#include <stdio.h>

using std::vector;

namespace dsm {

    StringPiece::StringPiece() : data(NULL), len(0) {
    }

    StringPiece::StringPiece(const StringPiece& s) : data(s.data), len(s.size()) {
    }

    StringPiece::StringPiece(const string& s) : data(s.data()), len(s.size()) {
    }

    StringPiece::StringPiece(const string& s, int len) : data(s.data()), len(len) {
    }

    StringPiece::StringPiece(const char* c) : data(c), len(strlen(c)) {
    }

    StringPiece::StringPiece(const char* c, int len) : data(c), len(len) {
    }

    uint32_t StringPiece::hash() const {
        return SuperFastHash(data, len);
    }

    string StringPiece::AsString() const {
        return string(data, len);
    }

    void StringPiece::strip() {
        while (len > 0 && isspace(data[0])) {
            ++data;
            --len;
        }
        while (len > 0 && isspace(data[len - 1])) {
            --len;
        }
    }

    static void StringPieceTestStrip() {
        StringPiece p = "abc def;";
        p.strip();
        assert(p.AsString() == "abc def;");

        StringPiece q = "   abc def;   ";
        q.strip();
        assert(q.AsString() == "abc def;");
    }
    REGISTER_TEST(StringPieceStrip, StringPieceTestStrip());

    vector<StringPiece> StringPiece::split(StringPiece sp, StringPiece delim) {
        vector<StringPiece> out;
        const char* c = sp.data;
        while (c < sp.data + sp.len) {
            const char* next = c;

            bool found = false;

            while (next < sp.data + sp.len) {
                for (int i = 0; i < delim.len; ++i) {
                    if (*next == delim.data[i]) {
                        found = true;
                    }
                }
                if (found)
                    break;

                ++next;
            }

            if (found || c < sp.data + sp.len) {
                StringPiece part(c, next - c);
                out.push_back(part);
            }

            c = next + 1;
        }

        return out;
    }

    static void StringPieceTestSplit() {
        vector<StringPiece> sp = StringPiece::split("a,b,c,d", ",");
        assert(sp[0].AsString() == "a");
        assert(sp[1].AsString() == "b");
        assert(sp[2].AsString() == "c");
        assert(sp[3].AsString() == "d");
    }
    REGISTER_TEST(StringPieceSplit, StringPieceTestSplit());

    string StringPrintf(StringPiece fmt, ...) {
        va_list l;
        va_start(l, fmt);
        string result = VStringPrintf(fmt, l);
        va_end(l);

        return result;
    }

    string VStringPrintf(StringPiece fmt, va_list l) {
        char buffer[32768];
        vsnprintf(buffer, 32768, fmt.AsString().c_str(), l);
        return string(buffer);
    }

}
