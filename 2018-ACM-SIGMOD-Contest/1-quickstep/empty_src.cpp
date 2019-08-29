// On OSX, ranlib complains if a static library archive contains no symbols,
// so we export a dummy global variable.
#ifdef __APPLE__
namespace project { extern constexpr int kDarwinGlobalDummy = 0; }
#endif
