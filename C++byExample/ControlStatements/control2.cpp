#include <iostream>
using namespace std;

int main()
{
  int a, b, c;
  cin >> a >> b;

  if (a == b) {
    cout << "a is equal with b";
  } else if (a < b) {
    cout << "a is less than b";
  } else if (a > b ) {
    cout << "a is great than b";
  }
  return 0;
}
