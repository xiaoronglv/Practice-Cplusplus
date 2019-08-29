#include<iostream>
#include<string>
using namespace std;

struct Person
{
  string name;
  int age;
};

void print(Person p)
{
  cout << p.name 
    << " is " 
    << p.age 
    << " years old."
    << endl;
}

int main(int argc, char *argv[])
{
  // Create two person
  Person you, me;
  me.name = "Ryan Lv";
  me.age = 33;
  print(me);

  cout << "What's your name?" << endl;
  cin >> you.name;
  cout << "how old are you?" << endl;
  cin >> you.age;
  print(you);
  return 0;
}
