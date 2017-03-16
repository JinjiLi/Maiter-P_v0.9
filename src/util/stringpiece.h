/* 
 * File:   stringpiece.h
 *  
 * Version 1.0
 *
 * Copyright [2016] [JinjiLi/ Northeastern University]
 * 
 * Author: JinjiLi<L1332219548@163.com>
 * 
 * Created on December 26, 2016, 3:40 PM
 */
#ifndef STRINGPIECE_H
#define	STRINGPIECE_H
#include <vector>
#include <string>
#include <memory.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>

namespace dsm {

using std::string;

class StringPiece {
public:
  StringPiece();
  StringPiece(const StringPiece& s);
  StringPiece(const string& s);
  StringPiece(const string& s, int len);
  StringPiece(const char* c);
  StringPiece(const char* c, int len);

  // Remove whitespace from either side
  void strip();

  uint32_t hash() const;
  string AsString() const;

  int size() const { return len; }

  const char* data;
  int len;

  static std::vector<StringPiece> split(StringPiece sp, StringPiece delim);
};

static bool operator==(const StringPiece& a, const StringPiece& b) {
  return a.data == b.data && a.len == b.len;
}

static const char* strnstr(const char* haystack, const char* needle, int len) {
  int nlen = strlen(needle);
  for (int i = 0; i < len - nlen; ++i) {
    if (strncmp(haystack + i, needle, nlen) == 0) {
      return haystack + i;
    }
  }
  return NULL;
}
//用空格连接字符串
template <class Iterator>
string JoinString(Iterator start, Iterator end, string delim=" ") {
  string out;
  while (start != end) {
    out += *start;
    ++start;
    if (start != end) { out += delim; }
  }
  return out;
}

#ifndef SWIG
string StringPrintf(StringPiece fmt, ...);
string VStringPrintf(StringPiece fmt, va_list args);
#endif

string ToString(int32_t);
string ToString(int64_t);
string ToString(string);
string ToString(StringPiece);

}

#include <tr1/functional_hash.h>

namespace std { namespace tr1 {
template <>
struct hash<dsm::StringPiece> : public unary_function<dsm::StringPiece, size_t> {
  size_t operator()(const dsm::StringPiece& k) const {
    return k.hash();
  }
};
}}


#endif	/* STRINGPIECE_H */

