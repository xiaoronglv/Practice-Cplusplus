#include <iostream>
using namespace std;

int main()
{
  char c;
  while(cin.get(c))
  {
    if (c >= 'a' && c <= 'z')
    {
        c = c + 'A' - 'a';
    }

    cout << c;
  }
  return 0;
}