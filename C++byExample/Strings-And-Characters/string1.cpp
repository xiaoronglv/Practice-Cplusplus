#include <iostream>
#include <string>

using namespace std;

int main()
{
  string s;

  do {
    cin >> s;
    cout << s << endl; 
  }
  while (s != "quit");
  return 0;
}
