#include <iostream>
#include <vector>
#include <string>

using namespace std;

template <class T>
class Stack {
  private:
    vector<T> elems;

  public:
    void push(T const&);
    void pop();
};

template <class T>
void Stack<T>::push(T const& elem) {
  elems.push_back(elem);
}

template <class T>
void Stack<T>::pop(){
  elems.pop_back();
}

int main(int argc, char *argv[])
{
  Stack<int> intStack;
  Stack<string> stringStack;

  intStack.push(7);
  stringStack.push("hello");
  return 0;
}
