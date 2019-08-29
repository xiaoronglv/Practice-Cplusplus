#include <iostream>
using namespace std;

void swap(int& a, int&b)
{
  int tmp = a;
  a = b;
  b = tmp;
}

int main(int argc, char *argv[])
{
  int a, b, c;
  cin >> a >> b >> c;

  if (a > b) {
    swap(a, b);
  }


  return 0;
}
