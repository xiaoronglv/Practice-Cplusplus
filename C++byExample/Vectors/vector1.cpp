#include<iostream>
#include <vector>
using namespace std;

int main(int argc, char *argv[])
{
  cout << "Please enter a senquence of integers:" << endl;
  vector<int> v;
  int x;
  while(cin >> x && x != 0)
  {
    v.push_back(x);
    cout << x << " has been successfully pushed into vector" << endl;
    cout << "The size of vector is " << v.size() << endl;
  };
  return 0;
}

