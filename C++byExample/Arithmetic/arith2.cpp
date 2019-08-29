#include <iostream>
using namespace std;

int main(int argc, char *argv[])
{
  int a = 0.0, b=0.0;
  cout << "Please enter two numbers:";
  cin >> a >> b;

  // Do arithmatics
  cout << a << "+" << b << "=" << a+b << endl;
  cout << a << "-" << b << "=" << a-b << endl;
  cout << a << "*" << b << "=" << a*b << endl;
  cout << a << "/" << b << "=" << a/b << endl;
  cout << a << "%" << b << "=" << a%b << endl;
  return 0;
}
