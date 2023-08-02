#!/bin/sh

set -e

seed=0
if [ $# -eq 1 ]; then
  seed=$1
fi

test_memory_internal() {
  ./test_memory_setup mem.db "$1" "$2" "$4" "$seed"
  valgrind --tool=massif --massif-out-file=massif.out ./test_memory mem.db "$1" "$2" "$3"
  usage=$(grep mem_heap_B massif.out | sed -e 's/mem_heap_B=\(.*\)/\1/' | sort -g | tail -n 1)
  usage=$((usage - 4096 * $3))
  max_usage=$((1024 * (100 + $3)))
  rm -f mem.db massif.out
  echo "usage: $usage"
  echo "max_usage: $max_usage"
  if [ $usage -gt $max_usage ]; then
    echo "test failed"
  else
    echo "test passed"
  fi
}

test_memory_internal 1 1 4 512
test_memory_internal 4 4 5 2048
test_memory_internal 16 16 8 8192
test_memory_internal 1 2 4 512
test_memory_internal 4 8 6 2048
test_memory_internal 16 32 9 8192
