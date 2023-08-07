# Hash Join (README modified by Nadim Isaac from CS564 Join Algorithm Project, credit to Professor Kevin Gaffney)

The join is a fundamental operation in relational data processing that finds matching rows between two tables. In this project, I will implement, test, and benchmark a disk-based join algorithm. The goal is to efficiently use memory and disk resources to return the answer to the following query.

```sql
SELECT R.b, S.b
FROM R, S
WHERE R.a = S.a;
```

## Algorithms

Two pass variant of Hash Join ..

### Sketches

#### Hash join (HJ)

- Partition $R$ using hash function $h_1$ and write partitions to disk.
- Partition $S$ using hash function $h_1$ and write partitions to disk.
- For each partition $(R_i, S_i)$:
	- For each tuple $r$ in $R_i$:
		- Insert $r$ into the hash table using hash function $h_2 \neq h_1$.
	- For each tuple $s$ in $S_i$:
		- Probe the hash table with $s$ using hash function $h_2 \neq h_1$. If $r.a = s.a$ then output $(r.b, s.b)$.

### Modifications

Single threaded permitted only.

## Implementation

The following function will be implemented in `src/join.cpp`.

```cpp
int join(File &file,
         int numPagesR,
         int numPagesS,
         char *buffer,
         int numFrames);
```

The function parameters are explained below.

- **`file`**: The database file. Refer to the documentation of the `File` class for more details.
- **`numPagesR`**: The number of pages in input table `R`.
- **`numPagesS`**: The number of pages in input table `S`.
- **`buffer`**: A contiguous memory region of buffer frames.
- **`numFrames`**: The number of frames in the buffer.

The function will return the number of tuples in the result.

### Input

The implementation will be run on randomly-generated databases. Each database will consist of two tables, `R` and `S`, each with two integer columns, `a` and `b`. I will evaluate the SQL query above.

Given $P_R$ pages in `R`, $P_S$ pages in `S`, and $B$ buffer frames, the following assumptions will be made about the input to your algorithm.

- The page size is 4096 bytes. The frame size is the same as the page size.
- $0 < P_R \le P_S$. The file consists of $P_R$ pages of table `R`, followed immediately by $P_S$ pages of table `S`, followed immediately by $P_R$ of empty output pages.
- $B \ge 2 + \sqrt{P_R + P_S}$. The number of frames in the buffer is large enough to achieve a two-pass algorithm for SMJ and HJ, given a reasonable hash function. The buffer is a contiguous memory region of $4096B$ bytes.
- The tuple size is 8 bytes. The first 4 bytes represent the value of column `a`. The last 4 bytes represent the value of column `b`. Values should be interpreted as unsigned 32-bit integers. There are 512 tuples per page. Each input table page is completely full. There are no empty slots.
- `R.a`, `S.a` $> 0$. There are no duplicate values in `R.a`. There are no duplicate values in `S.a`.


### Output

The output will be written to the pages that immediately follow table `S`. The output table will be packed. If there is a page in the output table with fewer than 512 tuples, it will be the last page in the table. Tuples are written in random order.


### Constraints

#### I/O cost

Given $P_R$ pages in `R`, $P_S$ pages in `S`, and $B$ buffer frames, the number of I/Os this implementation incurs will not exceed the following limits. 

| Algorithm | Maximum number of reads                                | Maximum number of writes |
| --------- | ------------------------------------------------------ | ------------------------ |
| HJ        | $2(P_R + P_S)$                                         | $2P_R + P_S$             |

#### Memory usage

Peak heap memory usage shall be less than or equal to $2^{10}(100 + B)$ bytes (not including the size of the buffer). Instructions for measuring peak heap memory usage are below.

## Building

Ensure you have the dependencies installed. To build the project, you only need CMake version 3.4 or newer and a C++ 17 enabled compiler. You can verify your CMake installation by running:

```bash
cmake --version
```

If CMake is installed on your system, you'll see output similar to `cmake version 3.x.x`. If you don't have CMake, follow these [installation instructions](https://cmake.org/install).

Clone the repository to your computer. If you're unfamiliar with GitHub, you can follow this [quick tutorial](https://docs.github.com/en/get-started/quickstart). Replace `URL` below with the GitHub URL of the repository.

```bash
git clone URL
```

Create and navigate into a build directory.

```bash
mkdir build && cd build
```

Generate the build files.

```bash
cmake ..
```

Finally, build the project.

```bash
cmake --build .
```

Optionally, run the tests. Some tests will fail if you have just started the project.

```bash
ctest .
```

To measure peak heap memory usage, run the provided script. This will only work on a Linux machine with Valgrind installed.

```bash
./test_memory.sh
```

### Tests

Tests are located in the subdirectory `src/test`. Tests for correctness and I/O cost of your implementation can be found in `src/test/test_join.cpp`.


## Leaderboard

Within each join algorithm (BNLJ, SMJ, HJ), the group with the fastest submission will receive 5 bonus points for this project. The group with the fastest overall submission (regardless of algorithm) will receive an additional 5 bonus points. Thus, you have the opportunity to earn up to 10 bonus points for this project. To be considered for the leaderboard, your implementation must be correct and satisfy the constraints on I/O cost and peak heap memory usage. Late submissions will not be considered.

To evaluate submissions, we will run your code repeatedly and compute the mean latency. If there are extremely close ties, we may award bonus points to multiple groups. In the performance evaluation database, `R` and `S` will have 100,000 pages each. The buffer will have 1000 frames.
