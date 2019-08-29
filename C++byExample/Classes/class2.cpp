#include <iostream>
#include <string>
using namespace std;

class Person
{
  private:
    string name;
    int age;
  public:
    Person();
    void read();
    void updateName(string name);
    void updateAge(int age);
};

Person::Person()
{
  name = "default";
  age = 33;
};

void Person::updateName(string name)
{
  name = name;
};

void Person::updateAge(int age)
{
  age = age;
};

void Person::read()
{
  cout << name << endl;
  cout << age << endl;
};

int main(int argc, char *argv[])
{
  Person me;
  me.read();
  return 0;
};
