#include <iostream> 
#include <vector> 
using namespace std; 
  
int main() 
{ 
    vector<int> myvector; 
    myvector.emplace_back(1); 
    myvector.emplace_back(2); 
    myvector.emplace_back(3); 
    myvector.emplace_back(4); 
    myvector.emplace_back(5); 
    myvector.emplace_back(6); 
    // vector becomes 1, 2, 3, 4, 5, 6 
  
    // printing the vector 
    for (auto it = myvector.begin(); it != myvector.end(); ++it) 
        cout << ' ' << *it; 
   
    return 0; 
      
} 
