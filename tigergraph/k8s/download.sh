#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. vars.sh
cd ..
tar -cf tmp.tar queries ddl dml k8s *.py key.json
for i in $( seq 0 $((NUM_NODES-1)) ); do
  echo "tigergraph-$i: Upload scripts"
  kubectl cp tmp.tar tigergraph-${i}:tmp.tar -n tigergraph
  echo "tigergraph-$i: Start download"
  kubectl exec tigergraph-${i} -n tigergraph -- bash -c \
      "tar -xf tmp.tar; 
      ./k8s/vars.sh; 
      export SERVICE_KEY=-k\ key.json;
      bash ./k8s/download_decompress.sh $i > log.download 2>&1 &"  
done
