# paramgen

1. Install dependencies:

    ```bash
    scripts/install-dependencies.sh
    ```

1. Generate factors with the Datagen. In the Datagen directory, issue:

    ```bash
    rm -rf out-sf${SF}/
    export SF=1
    tools/build.sh
    tools/run.py --cores 4 --memory 8G target/ldbc_snb_datagen_2.12_spark3.1-0.5.0-SNAPSHOT.jar -- --format csv --scale-factor ${SF} --explode-edges --mode bi --output-dir out-sf${SF}/ --generate-factors --format-options header=false,quoteAll=true
    ```

1. Move the factor directories from `out-sf${SF}/factors/csv/raw/composite-merged-fk/` (`cityPairsNumFriends/`, `personDisjointEmployerPairs/` etc.) to the `factors/` directory in this directory.

    ```bash
    mv ${LDBC_SNB_DATAGEN_DIR}/out-sf${SF}/factors/csv/raw/composite-merged-fk/* factors/
    ```

1. Run:

    ```bash
    scripts/paramgen.sh
    ```

1. The parameters will be placed in the `../parameters/` directory.
