#!/bin/bash
set -e 

# Set default container image if not provided
AG_IMAGE=${AG_IMAGE:-stijnking/avantgraph-mpg:latest}

# Represents (max_size, node_fraction, type_fraction)
for config in "1 0.01 0.012" "5 0.05 0.05" "10 0.25 0.1" "20 0.5 0.2"
do
    read -r max_size node_frac type_frac <<< "$config"
    echo "Measuring load time for config: max_size=${max_size}, node_frac=${node_frac}, type_frac=${type_frac}"

    FILEPATH="/data/snb-bi-${max_size}-${node_frac}-${type_frac}.json"

    docker run -it --rm -v ./data:/data -v ./scripts:/scripts --cap-add SYS_ADMIN --cap-add SYS_PTRACE --privileged ${AG_IMAGE} /scripts/create-schema.sh

    # Measure the loading time
    START_TIME=$(date +%s)
    
    docker run -it --rm -v ./data:/data -v ./scripts:/scripts --cap-add SYS_ADMIN --cap-add SYS_PTRACE --privileged -e DATA_FILE="${FILEPATH}" ${AG_IMAGE} /scripts/load-graph.sh

    END_TIME=$(date +%s)
    LOAD_TIME=$((END_TIME - START_TIME))

    echo "Load time for config (max_size=${max_size}, node_frac=${node_frac}, type_frac=${type_frac}): ${LOAD_TIME} seconds"

    # Export into log file
    echo "${max_size},${node_frac},${type_frac},${LOAD_TIME}" >> ./data/load_times.csv
done
