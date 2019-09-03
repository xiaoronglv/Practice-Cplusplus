#include<iostream>
using namespace std;

template <typename T>
T myMax(T x, T y)
{
  return (x > y) ? x : y;
}

int main(int argc, char *argv[])
{
  cout << myMax(3, 7) <<  endl;
  cout << myMax("a", "b") << endl;
  return 0;
}
