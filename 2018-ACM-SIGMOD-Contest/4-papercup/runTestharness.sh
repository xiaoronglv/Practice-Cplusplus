#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

#WORKLOAD_DIR=${1-$DIR/workloads/large}
#WORKLOAD_DIR=${1-$DIR/workloads/tiny2}
WORKLOAD_DIR=${1-$DIR/workloads/small}
WORKLOAD_DIR=$(echo $WORKLOAD_DIR | sed 's:/*$::')

cd $WORKLOAD_DIR

WORKLOAD=$(basename "$PWD")
echo execute $WORKLOAD ...

# .init contains all the filenames of relations
# .work contains all the queries
# .result are the expected results for testing.

# $DIR/build/release/harness *.init *.work *.result ../../run.sh
$DIR/build/release/harness *.init small.work small.result ../../run.sh
