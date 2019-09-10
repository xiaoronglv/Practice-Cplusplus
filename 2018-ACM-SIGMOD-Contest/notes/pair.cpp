#include<iostream>
#include<string>

using namespace std;

int main(int argc, char *argv[])
{
  pair <string, string> couple;
  couple.first = "Ryan Lv";
  couple.second = "Angela Liu";

  cout << "husband" << couple.first << endl;
  cout << "wife" << couple.second << endl;
  return 0;
}
