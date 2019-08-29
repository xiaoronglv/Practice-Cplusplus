#include<iostream>
#include<string>

using namespace std;

class People
{
  public:
    void operator +=(string s);
};

void People::operator +=(string s){
  cout << s << endl;
}
  
int main(int argc, char *argv[])
{
  People me;
  me += "hello";
  return 0;
}
