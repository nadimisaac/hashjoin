#include "join.hpp"
#include <array>
#include <cstdint>
#include <vector>
#include <cmath>
#include <iostream>
#include <cstring>

JoinAlgorithm getJoinAlgorithm()
{
  // TODO: Return your chosen algorithm.
  // return JOIN_ALGORITHM_BNLJ;
  // return JOIN_ALGORITHM_SMJ;
  return JOIN_ALGORITHM_HJ;
  throw std::runtime_error("not implemented: getJoinAlgorithm");
};

int join(File &file, int numPagesR, int numPagesS, char *buffer,
         int numFrames)
{
  // TODO: Implement your chosen algorithm.
  // Currently, this function performs a nested loop join in memory. While it
  // will pass the correctness and I/O cost tests, it ignores the provided
  // buffer and instead reads table data into vectors. It will not satisfy the
  // constraints on peak heap memory usage for large input tables. You should
  // delete this code and replace it with your disk-based join algorithm
  // implementation.


  int pageIndexR = 0;
  int pageIndexS = pageIndexR + numPagesR;
  int pageIndexOut = pageIndexS + numPagesS;
  int pageIndexRPartitioned = pageIndexOut + numPagesR;
  int numPartitions = (int)ceil((double) numPagesS / (numFrames - 2)); // under assumption that S is larger than R

  if (numPartitions > numFrames - 2) {
    numPartitions = numFrames - 2;
  }

  int numPagesPerRPartition = ceil(1.2 * numPagesR / numPartitions); // 1.2 fudge factor
  int numPagesPerSPartition = ceil(1.2 * numPagesS / numPartitions); // 1.2 fudge factor
  int pageIndexSPartitioned = pageIndexRPartitioned + (numPagesPerRPartition * (numPartitions));



  int numTuples = 0;
  int noPartitioning = 0;

  std::vector<int> tuplesPerPartitionR((numPartitions), 0);
  std::vector<int> tuplesPerPartitionS((numPartitions), 0);

  if (numPartitions == 1) {
    noPartitioning = 1;
  }

  // iterate through every page in R from file
  for (int pageIdxR = pageIndexR; pageIdxR < numPagesR; pageIdxR++)
  {
    // read this page into the first frame of the buffer
    file.read(buffer, pageIdxR, 1);
    Tuple *tuples = (Tuple *)buffer;

    // iterate through every tuple `r` of the input buffer frame page
    for (int i = 0; i < 512; i++)
    {
      Tuple *r = &tuples[i];
      uint32_t r_a = r->first; // joining on this value, so we use it to hash

      int r_partition_index = 0;

      if (noPartitioning) {
        r_partition_index = 0;
      }

      else {
        r_partition_index = r_a % numPartitions;
      }

      // we will need to place this tuple at frame number (r_partition_index + 1) in buffer
      Tuple *frame = (Tuple *)&buffer[4096 * (r_partition_index + 1)];

      // now that we know which buffer frame we can place it in, we must determine which slot in the frame to place it in
      frame[tuplesPerPartitionR[r_partition_index] % 512] = *r;
      tuplesPerPartitionR[r_partition_index] += 1; // increment this by one, to indicate that we have added a tuple to this buffer frame

      // if buffer frame gets filled up completely, its time to flush it to disk
      if (tuplesPerPartitionR[r_partition_index] % 512 == 0)
      {
        int filePageIndex = pageIndexRPartitioned + (numPagesPerRPartition * r_partition_index) + (tuplesPerPartitionR[r_partition_index] / 512) - 1;
        file.write(&buffer[4096 * (r_partition_index + 1)], filePageIndex, 1);
        memset(&buffer[4096 * (r_partition_index + 1)], -1, 1);
      }
    }
  }

  // at this point, we have formed partitions for R and flushed them to disk, however, there may be some pages in buffer
  // that have not finished filling up and, as a result were never flushed
  // we can identify these pages in buffer by checking whether the first byte of each page in buffer is equal to -1
  // let's go ahead and do a clean up flush !

  int frameIndex = 1;
  for (frameIndex; frameIndex < numPartitions + 1; frameIndex++)
  {
    int r_partition_index = frameIndex - 1;
    if (tuplesPerPartitionR[r_partition_index] == 0)
    {
      continue;
    }

    if (buffer[4096 * frameIndex] != -1)
    {
      int filePageIndex = pageIndexRPartitioned + (numPagesPerRPartition * r_partition_index) + (tuplesPerPartitionR[r_partition_index] / 512);
      file.write(&buffer[4096 * (r_partition_index + 1)], filePageIndex, 1);
      memset(&buffer[4096 * (r_partition_index + 1)], -1, 1);
    }
  }

  // iterate through every page in S from file
  for (int pageIdxS = pageIndexS; pageIdxS < pageIndexOut; pageIdxS++)
  {
    // read this page into the first frame of the buffer
    file.read(buffer, pageIdxS, 1);
    Tuple *tuples = (Tuple *)buffer;

    // iterate through every tuple `s` of the input buffer frame page
    for (int i = 0; i < 512; i++)
    {
      Tuple *s = &tuples[i];
      uint32_t s_a = s->first; // joining on this value, so we use it to hash

      int s_partition_index = 0;

      if (noPartitioning) {
        s_partition_index = 0;
      }

      else {
        s_partition_index = s_a % numPartitions;
      }

      // we will need to place this tuple at frame number (s_partition_index + 1) in buffer
      Tuple *frame = (Tuple *)&buffer[4096 * (s_partition_index + 1)];

      // now that we know which buffer frame we can place it in, we must determine which slot in the frame to place it in
      frame[tuplesPerPartitionS[s_partition_index] % 512] = *s;
      tuplesPerPartitionS[s_partition_index] += 1; // increment this by one, to indicate that we have added a tuple to this buffer frame

      // if buffer frame gets filled up completely, its time to flush it to disk
      if (tuplesPerPartitionS[s_partition_index] % 512 == 0)
      {
        int filePageIndex = pageIndexSPartitioned + (numPagesPerSPartition * s_partition_index) + (tuplesPerPartitionS[s_partition_index] / 512) - 1;
        file.write(&buffer[4096 * (s_partition_index + 1)], filePageIndex, 1);
        memset(&buffer[4096 * (s_partition_index + 1)], -1, 1);
      }
    }
  }

  // at this point, we have formed partitions for S and flushed them to disk, however, there may be some pages in buffer
  // that have not finished filling up and, as a result were never flushed
  // we can identify these pages in buffer by checking whether the first byte of each page in buffer is equal to -1
  // let's go ahead and do a clean up flush !

  frameIndex = 1;
  for (frameIndex; frameIndex < numPartitions + 1; frameIndex++)
  {
    if (buffer[4096 * frameIndex] != -1)
    {
      int s_partition_index = frameIndex - 1;
      int filePageIndex = pageIndexSPartitioned + (numPagesPerSPartition * s_partition_index) + (tuplesPerPartitionS[s_partition_index] / 512);
      file.write(&buffer[4096 * (s_partition_index + 1)], filePageIndex, 1);
      memset(&buffer[4096 * (s_partition_index + 1)], -1, 1);
    }
  }

  // at this point, we have hashed partitions of R and S on disk

  // Hash table in memory... B buffer frames
  // First buffer frame dedicated for reading in a page, whether that be a page of R or a page of S

  // let's clean up our buffer first... this will be useful for later determining whether hash slot collides or not
  memset(buffer, -1, 4096 * numFrames);

  // BUILDING AND MATCHING

  // iterate through each partition, i, for R and S together... partitions may contain a different number of pages
  for (int i = 0; i < numPartitions; i++)
  {
    int numPagesRi = (int)ceil((double)(tuplesPerPartitionR[i]) / 512);
    int numPagesSi = (int)ceil((double)(tuplesPerPartitionS[i]) / 512);

    if (numPagesRi == 0 or numPagesSi == 0)
    {
      continue;
    }

    // iterate through every page in partition Ri from file, reading each page into memory
    // construct a hash table on all the tuples from all the pages in this partition
    for (int j = 0; j < numPagesRi; j++)
    {
      int RiPageIndex = pageIndexRPartitioned + (numPagesPerRPartition * i) + j;
      file.read(buffer, RiPageIndex, 1);

      // at first frame of buffer, we now have read in the page which we are hashing
      // at last frame of buffer, we designate for output
      // B - 2 Frames left
      // let's acquire a hash function for hash range 0 to (B - 2) * 512
      // we will add one frame upon receiving our hash index, to skip past input buffer frame

      Tuple *tuples = (Tuple *)buffer;

      // iterate through every tuple `r` in this page of the partition
      // sometimes there will not be 512 tuples in the page
      // this will happen when we are on the last page of the partition
      // as a result, we check if we are on the last page, and set the num of tuples to iterate accordingly
      int tuplesInPage = 0;

      if (j == numPagesRi - 1)
      {
        if (tuplesPerPartitionR[i] % 512 == 0)
        {
          tuplesInPage = 512;
        }

        else
        {
          tuplesInPage = tuplesPerPartitionR[i] % 512;
        }
      }

      else
      {
        tuplesInPage = 512;
      }

      for (int z = 0; z < tuplesInPage; z++)
      {
        Tuple *r = &tuples[z];
        uint32_t r_a = r->first; // joining on this value, so we use it to hash

        // hash our key, and insert into hash table
        int tuple_hash_index = int(((numFrames - 2) * 512) * (r_a * 0.618304 - uint32_t(r_a * 0.618304)));
         //int tuple_hash_index = r_a % ((numFrames - 2) * 512);

        Tuple *entry = &tuples[tuple_hash_index + 512];
        if (entry->first != -1)
        { // collision ! ... let's implement open addressing using linear probing

          while (true)
          {
            entry++;

            if (entry == (Tuple *)&buffer[4096 * (numFrames - 1)])
            {
              entry = (Tuple *)&buffer[4096]; // just past first input buffer frame
            }

            if (entry->first == -1)
            { // we have found an empty slot
            *entry = *r;
            break;
            }

            else
            {
              continue;
            }
          }
        }
        // now we know which tuple index to place it in the buffer... this is its hashed location

        else
        {
          *entry = *r;
        }
      }
    }

    // iterate through every page in partition Si from file, reading each page into memory
    for (int j = 0; j < numPagesSi; j++)
    {
      int SiPageIndex = pageIndexSPartitioned + (numPagesPerSPartition * i) + j;
      file.read(buffer, SiPageIndex, 1);

      Tuple *tuples = (Tuple *)buffer;

      // iterate through every tuple `s` in this page of the partition

      int tuplesInPage = 0;

      if (j == numPagesSi - 1)
      {
        if (tuplesPerPartitionS[i] % 512 == 0)
        {
          tuplesInPage = 512;
        }

        else
        {
          tuplesInPage = tuplesPerPartitionS[i] % 512;
        }
      }
      else
      {
        tuplesInPage = 512;
      }

      for (int z = 0; z < tuplesInPage; z++)
      {
        Tuple *s = &tuples[z];
        uint32_t s_a = s->first; // joining on this value, so we use it to probe

        // hash our key, and insert into hash table
        int tuple_probe_index = int(((numFrames - 2) * 512) * (s_a * 0.618304 - uint32_t(s_a * 0.618304)));
        // int tuple_probe_index = s_a % ((numFrames - 2) * 512);

        Tuple *entry = &tuples[tuple_probe_index + 512];

        int probeMatch = 0;

        // we hash probed to a slot which is empty... therefore, there is no match
        if (entry->first == -1)
        {
         probeMatch = 0; // probeMatch stays equal to 0
        }
        

        // if we hash to an index where the slot is non empty and does not equal our match
        // we still have the possibility of probing a match
        // we can linear probe to get to match...
        // on which we stop linear probing on three cases:
        // 1) we find a match
        // 2) we reach an empty slot ... no match
        // 3) we wrap around back to the same slot we hashed to in the first place
        else if (entry->first != s_a)  
        { 

          while (true)
          {
            entry++;

          if (entry == (Tuple *)&buffer[4096 * (numFrames - 1)]) {
              // we have probed all the way to output frame
              // let's wraparound to just past first input buffer frame
              // continue probing
              entry = (Tuple *)&buffer[4096];
              
            }

            if (entry->first == s_a) {
              // we linear probed to a match
              probeMatch = 1;
              break;
            }

             if (entry->first == -1)
            { // we have reached an empty slot, so probed key is not in this hash table.. no match
            probeMatch = 0;
            break;
            }

          }

        }

        else if (entry->first == s_a) {
          // hashed to a match
          probeMatch = 1;
        }

        // now we are at probed index entry, using s's key value
        // if the entry's first attribute is equal to s's first attribute, we have ourselves a match
        // else continue
        if (probeMatch)
        {
          // match found, place in output buffer and check for a write
          Tuple *lastFrame = (Tuple *)&buffer[4096 * (numFrames - 1)];
          Tuple myTuple = Tuple(entry->second, s->second);
          lastFrame[numTuples % 512] = myTuple;
          numTuples++;

          if (numTuples % 512 == 0 && numTuples != 0)
          {
            // write output frame to file
            int outPageIndex = pageIndexOut + (numTuples / 512) - 1;
            file.write(&buffer[4096 * (numFrames - 1)], outPageIndex, 1);
            memset(&buffer[4096 * (numFrames - 1)], -1, 1);
          }
        }
      }
    }

    // after scanning all tuples within this all partitions of S, our output buffer may have not filled up completely, and we can have
    // a partial page filled up of output
    // we need to flush this

    // clear hash table for next partition, unless this is the final iteration

    if (i < numPartitions - 1)
    {
      memset(buffer, -1, 4096 * (numFrames - 1));
    }
  }

  // after scanning all tuples within this all partitions of S, our output buffer may have not filled up completely, and we can have
  // a partial page filled up of output
  // we need to flush this

  // we do not want to flush anything if we do not

  // write output frame to file after all partitions, if we have a partial output frame
  if (numTuples % 512 != 0)
  {
    int outPageIndex = pageIndexOut + (numTuples / 512);
    file.write(&buffer[4096 * (numFrames - 1)], outPageIndex, 1);
  }

  return numTuples;
}
