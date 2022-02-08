# paramgen

1. Install dependencies:

    ```bash
    scripts/install-dependencies.sh
    ```

1. Generate factors with the Datagen:

    ```bash
    tools/build.sh
    tools/run.py --cores 4 --memory 8G target/ldbc_snb_datagen_2.12_spark3.1-0.5.0-SNAPSHOT.jar -- --format csv --scale-factor 1 --mode raw --output-dir out
    tools/run.py --cores 4 --memory 8G --main-class ldbc.snb.datagen.factors.FactorGenerationStage target/ldbc_snb_datagen_2.12_spark3.1-0.5.0-SNAPSHOT.jar -- --output-dir out
    ```

1. Move the factor directories from `out/factors/csv/raw/composite-merged-fk/` (`cityPairsNumFriends/`, `personDisjointEmployerPairs/` etc.) to the `factors/` directory.

1. Run:

    ```bash
    scripts/paramgen.sh
    ```
