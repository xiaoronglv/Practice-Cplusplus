#include<iostream>

using namespace std;

int main(int argc, char *argv[])
{
  int a = 1;
  __sync_fetch_and_add(&a, 1);
  cout << a << endl;
  return 0;
}
