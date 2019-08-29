#include <iostream>
#include <string>
using namespace std;

int main()
{
  string s;
  while (getline(cin, s))
    cout << s << endl << "it's a new line" << endl;
  return 0;
}
