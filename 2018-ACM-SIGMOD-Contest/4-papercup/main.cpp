#include <iostream>
#include <thread>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "Config.hpp"

using namespace std;

int main(int argc, char* argv[]) {
    Joiner joiner(THREAD_NUM);
    // Read join relations
    string line;
    while (getline(cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }
    joiner.loadStat();
    //chrono::steady_clock::time_point begin = chrono::steady_clock::now();


    while (getline(cin, line)) {
        //if (line == "F") continue; // End of a batch
        if (line == "F") { // End of a batch
            joiner.waitAsyncJoins();
            auto results = joiner.getAsyncJoinResults(); // result strings vector
            for (auto& result : results)
                cout << result;
            continue;
        }
        joiner.createAsyncQueryTask(line);
    }
    return 0;
}
