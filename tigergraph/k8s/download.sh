#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. vars.sh
cd ..
tar -cf tmp.tar queries ddl dml k8s *.py
for i in $( seq 0 $((NUM_NODES-1)) ); do
  echo "tigergraph-$i: Upload scripts"
  kubectl cp tmp.tar tigergraph-${i}:tmp.tar 
  #echo "tigergraph-$i: Start download"
  #kubectl exec tigergraph-${i} -- bash -c "tar -xf tmp.tar; sh ./k8s/download_one_pod.sh $i > log.download 2> /dev/null &"  
done
