#include <iostream>
#include "Joiner.hpp"
#include "Parser.hpp"

using namespace std;
//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
   Joiner joiner;
   // Read join relations
   string line;

   FILE* pFile = fopen("logFile.txt", "a");
  
   while (getline(cin, line)) {
      fprintf(pFile, "%s\n",line.c_str());
      if (line == "Done") break;
      joiner.addRelation(line.c_str());
   }
   // Preparation phase (not timed)
   // Build histograms, indexes,...
   //
   QueryInfo i;
   while (getline(cin, line)) {
      fprintf(pFile, "%s\n",line.c_str());
      if (line == "F") continue; // End of a batch
      i.parseQuery(line);
      cout << joiner.join(i);
   }
   return 0;
}
