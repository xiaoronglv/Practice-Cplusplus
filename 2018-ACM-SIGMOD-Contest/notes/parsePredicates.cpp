


// example: "0 2 4|0.1=1.2&1.0=2.1&0.1>3000|0.0 1.1"
void parsePredicates(string& text)
// Parse predicates
{
    vector<string> predicateStrings;
    splitString(text,predicateStrings,'&');
    for (auto& rawPredicate : predicateStrings) {
        parsePredicate(rawPredicate);
   }
}

int main(int argc, char *argv[])
{
  
  return 0;
}
