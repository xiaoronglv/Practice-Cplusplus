#include <iostream>
using namespace std;

int main()
{
  cout << "Enter some text. Type CTRL-C to quit" << endl; 
  char c;
  int a = 0;
  while (cin >> c)
  {
    a++;
    cout << a << endl;
    cout << c << endl;
}
  return 0;
}
