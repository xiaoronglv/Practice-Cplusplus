#include<iostream>
#include<vector>
#include<string>
#include<sstream>
using namespace std;

static void splitString(string& line,vector<string>& result,const char delimiter)
// Parse a line into strings
{
    stringstream ss(line);
    string token;
    while (getline(ss,token,delimiter)) {
        result.push_back(token);
    }
}
//---------------------------------------------------------------------------
static void splitPredicates(string& line, vector<string>& result)
// Split a line into predicate strings
{
    vector<char> comparisonTypes{'>', '<', '='};
    // Determine predicate type
    for (auto cT : comparisonTypes) {
        if (line.find(cT)!=string::npos) {
            splitString(line,result,cT);
            break;
        }
    }
}

int main(int argc, char *argv[])
{
  string line = "0.1=1.2"; 
  vector<string> result;
  splitPredicates(line, result);

  cout << result[0] << endl;
  cout << result[1] << endl;
  cout << result[2] << endl;
  return 0;
}
