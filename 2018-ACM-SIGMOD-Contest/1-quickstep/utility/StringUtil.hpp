#ifndef PROJECT_UTILITY_STRING_UTIL_HPP_
#define PROJECT_UTILITY_STRING_UTIL_HPP_

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string>
#include <vector>

namespace project {

inline std::string ToLower(const std::string &str) {
  std::string lower_str(str.size(), ' ');
  std::transform(str.begin(), str.end(), lower_str.begin(), tolower);
  return lower_str;
}

inline std::string ToUpper(const std::string &str) {
  std::string upper_str(str.size(), ' ');
  std::transform(str.begin(), str.end(), upper_str.begin(), toupper);
  return upper_str;
}

template <typename ContainerType>
inline std::string ConcatToString(const ContainerType &container,
                                  const std::string &separator = "") {
  std::ostringstream oss;
  bool is_first = true;
  for (const auto &item : container) {
    if (is_first) {
      is_first = false;
    } else {
      oss << separator;
    }
    oss << item;
  }
  return oss.str();
}

inline std::string TrimLeft(const std::string &str) {
  return str.substr(str.find_first_not_of(" \f\n\r\t\v"));
}

inline std::string TrimRight(const std::string &str) {
  return str.substr(0, str.find_last_not_of(" \f\n\r\t\v")+1);
}

inline std::string Trim(const std::string &str) {
  return TrimLeft(TrimRight(str));
}

inline std::vector<std::string> SplitString(const std::string &str,
                                            const char delimiter) {
  std::vector<std::string> results;
  std::istringstream iss(str);
  std::string token;
  while (std::getline(iss, token, delimiter)) {
    results.emplace_back(token);
  }
  return results;
}

inline std::string EscapeSpecialChars(const std::string& text) {
  std::string new_text;
  for (const char& c : text) {
    switch (c) {
      case '\'':
        new_text.append("\\\'");
        break;
      case '\"':
        new_text.append("\\\"");
        break;
      case '\?':
        new_text.append("\\\?");
        break;
      case '\t':
        new_text.append("\\t");
        break;
      case '\a':
        new_text.append("\\a");
        break;
      case '\b':
        new_text.append("\\b");
        break;
      case '\f':
        new_text.append("\\f");
        break;
      case '\n':
        new_text.append("\\n");
        break;
      case '\r':
        new_text.append("\\r");
        break;
      case '\v':
        new_text.append("\\v");
        break;
      default:
        new_text += c;
    }
  }
  return new_text;
}

}  // namespace project

#endif  // PROJECT_UTILITY_STRING_UTIL_HPP_
