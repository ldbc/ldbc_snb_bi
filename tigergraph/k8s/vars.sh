# SF-100 benchmark
export NUM_NODES=2 # number of pods or nodes
export SF=100 # data source 100, 1l, 3k, 10k, 30k ...
export DOWNLOAD_THREAD=5 # number of download threads
export TG_HEADER=true # whether data has header
export NRUNS=10 # number of query runs

# SF-10k benchmark 
use_sf10k() {
  export NUM_NODES=16
  export SF=10k
  export DOWNLOAD_THREAD=10
  export TG_HEADER=true
  export NRUNS=3
}

#use_sf10k