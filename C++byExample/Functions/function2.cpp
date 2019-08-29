#include<iostream>
using namespace std;

int max(int a, int b)
{
  if (a <= b) 
    return b;
  else
    return a;
}

int main(int argc, char *argv[])
{
  int a, b;
  cin >> a >> b;
  cout << max(a, b) << endl;
  return 0;
}
