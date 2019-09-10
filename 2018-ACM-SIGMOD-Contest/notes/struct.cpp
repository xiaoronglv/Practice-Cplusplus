#include<iostream>
#include<string>
using namespace std;

struct Point { double x, y; };

int main(int argc, char *argv[])
{
  struct Point p;
  p.x = 3.0;
  cout << p.x << p.y << endl;
  return 0;
}
