#include <iostream>

#include "utility/Range.hpp"
#include "utility/StringUtil.hpp"

using namespace project;  // NOLINT[build/namespaces]

int main(int argc, char *argv[]) {
  const RangeSplitter splitter =
      RangeSplitter::CreateWithPartitionLength(0, 12345, 1000);

  std::cout << "# partitions = " << splitter.size() << "\n";
  for (const Range range : splitter) {
    std::cout << range.begin() << " -- " << range.end() << "\n";
  }

  std::cout << ConcatToString(Range(10, 20).getGenerator(), ", ") << "\n";
}
