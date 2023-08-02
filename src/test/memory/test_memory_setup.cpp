#include "file.hpp"
#include "join.hpp"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

void usage(char **argv) {
  std::cerr << "Usage: " << argv[0]
            << " filename numPagesR numPagesS numTuplesShared seed"
            << std::endl;
}

int main(int argc, char **argv) {
  if (argc < 6) {
    usage(argv);
    return 1;
  }

  std::string filename = argv[1];
  int numPagesR = std::stoi(argv[2]);
  int numPagesS = std::stoi(argv[3]);
  int numTuplesShared = std::stoi(argv[4]);
  int seed = std::stoi(argv[5]);

  std::minstd_rand rng(seed);
  std::uniform_int_distribution<uint32_t> dis(1, UINT32_MAX);

  int numTuplesR = numPagesR * 512;
  int numTuplesS = numPagesS * 512;

  std::vector<Tuple> tupleVectorShared;
  tupleVectorShared.reserve(numTuplesShared);

  std::unordered_set<uint32_t> keysShared;
  keysShared.reserve(numTuplesShared);
  while (keysShared.size() < numTuplesShared) {
    keysShared.emplace(dis(rng));
  }

  std::unordered_map<uint32_t, uint32_t> tupleSetR;
  tupleSetR.reserve(numTuplesR);

  std::unordered_map<uint32_t, uint32_t> tupleSetS;
  tupleSetS.reserve(numTuplesS);

  for (uint32_t a : keysShared) {
    uint32_t bR = dis(rng);
    uint32_t bS = dis(rng);
    tupleVectorShared.emplace_back(bR, bS);
    tupleSetR.emplace(a, bR);
    tupleSetS.emplace(a, bS);
  }

  while (tupleSetR.size() < numTuplesR) {
    uint32_t a = dis(rng);
    if (tupleSetR.find(a) == tupleSetR.end()) {
      tupleSetR.emplace(a, dis(rng));
    }
  }

  while (tupleSetS.size() < numTuplesS) {
    uint32_t a = dis(rng);
    if (tupleSetR.find(a) == tupleSetR.end() &&
        tupleSetS.find(a) == tupleSetS.end()) {
      tupleSetS.emplace(a, dis(rng));
    }
  }

  std::vector<Tuple> tupleVectorR(tupleSetR.begin(), tupleSetR.end());
  std::shuffle(tupleVectorR.begin(), tupleVectorR.end(), rng);

  std::vector<Tuple> tupleVectorS(tupleSetS.begin(), tupleSetS.end());
  std::shuffle(tupleVectorS.begin(), tupleVectorS.end(), rng);

  File file(filename);
  file.write(tupleVectorR.data(), 0, numPagesR);
  file.write(tupleVectorS.data(), numPagesR, numPagesS);

  return 0;
}
