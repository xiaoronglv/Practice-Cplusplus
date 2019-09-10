#include<iostream>
#include<string>
#include<vector>
#include<sstream>
using namespace std;

static void splitString(string& line,vector<unsigned>& result,const char delimiter)
// Split a line into numbers
{
    stringstream ss(line);
    string token;
    while (getline(ss,token,delimiter)) {
        result.push_back(stoul(token));
    }
}

int main(int argc, char *argv[])
{
  vector<unsigned> result;
  string s = "0 2 4";
  splitString(s, result, ' ');
  cout << result[0] << endl;
  cout << result[1] << endl;
  cout << result[2] << endl;
  return 0;
}
