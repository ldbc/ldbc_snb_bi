import os
import duckdb

sf = os.environ.get("SF")
if sf is None:
    print("${SF} environment variable must be set")
    exit(1)

# Use an on-disk database to allow spilling to disk
con = duckdb.connect(database='scratch/factors.duckdb')
 
factor_parquet_path = "scratch/factors/"

print("============ Loading the factor tables ============")
for entity in ["cityNumPersons", "cityPairsNumFriends", "companyNumEmployees", "countryNumMessages", "countryNumPersons", "countryPairsNumFriends", "creationDayAndLengthCategoryNumMessages", "creationDayAndTagNumMessages", "creationDayAndTagClassNumMessages", "creationDayNumMessages", "languageNumPosts", "lengthNumMessages", "people2Hops", "people4Hops", "personDisjointEmployerPairs", "personNumFriends", "tagClassNumMessages", "tagClassNumTags", "tagNumMessages", "tagNumPersons", "sameUniversityConnected"]:
    print(f"{entity}")
    con.execute(f"CREATE OR REPLACE TABLE {entity} AS SELECT * FROM read_parquet('{factor_parquet_path}{entity}/*.parquet')")

print()
print("============ Loading the temporal tables ============")
for entity in ["personDays", "personKnowsPersonDays", "personStudyAtUniversityDays", "personWorkAtCompanyDays"]:
    print(f"{entity}")
    con.execute(f"""
        CREATE OR REPLACE TABLE {entity}_window AS
            SELECT * EXCLUDE (creationDay, deletionDay)
            FROM read_parquet('{factor_parquet_path}{entity}/*.parquet')
            WHERE creationDay < TIMESTAMP '2012-11-29'
              AND deletionDay > TIMESTAMP '2013-01-01'
        """)

# Insert knows edges backwards
con.execute(f"""
    INSERT INTO personKnowsPersonDays_window
        SELECT Person2Id, Person1Id
        FROM personKnowsPersonDays_window
    """)

# Generating parameters
print()
print("============ Generating parameters ============")
for paramgen_query_id in [
    "1", "2_tagClassAndWindowNumMessages", "2a", "2b", "3", "4", "5", "6", "7", "8_tagAndWindowNumMessages", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a_core", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20_sameUniversityKnows", "20b_comp", "20b_twohop", "20b"]:
    print(f"- Q{paramgen_query_id}")
    with open(f"paramgen-queries/pg-{paramgen_query_id}.sql", "r") as parameter_query_file:
        paramgen_query_content = parameter_query_file.read()
        con.execute(f"CREATE OR REPLACE TABLE q{paramgen_query_id} AS {paramgen_query_content}")

# Copy the generated parameters into CSV files
print()
print("============ Serializing parameters ============")
for query_variant in ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"]:
    print(f"- Q{query_variant}")
    with open(f"paramgen-queries/pg-{query_variant}.sql", "r") as parameter_query_file:
        con.execute(f"COPY q{query_variant} TO '../parameters/parameters-sf{sf}/bi-{query_variant}.csv' WITH (HEADER, DELIMITER '|')")
