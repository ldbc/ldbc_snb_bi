# Parameter generation

The paramgen implements [parameter curation](https://research.vu.nl/en/publications/parameter-curation-for-benchmark-queries) to ensure predictable performance results that (mostly) correspond to a normal distribution.

## Getting started

1. Install dependencies:

    ```bash
    scripts/install-dependencies.sh
    ```

1. Generate factors with the Datagen. In Datagen's directory (`ldbc_snb_datagen_spark`), issue the following commands. We assume that the Datagen project is built and the `${PLATFORM_VERSION}`, `${DATAGEN_VERSION}` environment variables are set correctly.

    ```bash
    export SF=desired_scale_factor
    ```

    ```bash
    rm -rf out-sf${SF}/
    tools/run.py \
        --cores 4 \
        --memory 8G \
        ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar \
        -- \
        --format csv \
        --scale-factor ${SF} \
        --mode bi \
        --output-dir out-sf${SF} \
        --generate-factors
    ```

1. Move the factor directories from `out-sf${SF}/factors/csv/raw/composite-merged-fk/` (`cityPairsNumFriends/`, `personDisjointEmployerPairs/`, etc.) to the `factors/` directory in this directory. If your `${LDBC_SNB_DATAGEN_DIR}` and `${SF}` environment variables are set, simply run:

    ```bash
    cp -r ${LDBC_SNB_DATAGEN_DIR}/out-sf${SF}/factors/csv/raw/composite-merged-fk/* factors/
    ```

1. Run:

    ```bash
    scripts/paramgen.sh
    ```

1. The parameters will be placed in the `../parameters/` directory.
