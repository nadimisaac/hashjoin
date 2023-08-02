#include "file.hpp"
#include "join.hpp"

#include <iostream>
#include <numeric>
#include <string>

void usage(char **argv) {
  std::cerr << "Usage: " << argv[0] << " filename numPagesR numPagesS numFrames"
            << std::endl;
}

int main(int argc, char **argv) {
  if (argc < 5) {
    usage(argv);
    return 1;
  }

  std::string filename = argv[1];
  int numPagesR = std::stoi(argv[2]);
  int numPagesS = std::stoi(argv[3]);
  int numFrames = std::stoi(argv[4]);

  char *buffer = new char[numFrames * 4096];
  File file(filename);
  join(file, numPagesR, numPagesS, buffer, numFrames);

  return 0;
}
