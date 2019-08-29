#include <iostream>
using namespace std;

int main()
{
    /* code */
    cout << "Please enter your text. Type CTRL+C to quit" << endl;
    char c;
    while (cin.get(c))
    {
        cout << c;
    }
    
    return 0;
}



