import os
import duckdb

con = duckdb.connect(database='scratch/factors.duckdb')
 
factor_parquet_path = "factors/"
temporal_parquet_path = "temporal/"

print("============ Initializing database ============")
with open(f"paramgen-queries/ddl/schema.sql", "r") as schema_file:
    con.execute(schema_file.read())

print()
print("============ Loading the factor tables ============")
for entity in ["cityNumPersons", "cityPairsNumFriends", "companyNumEmployees", "countryNumMessages", "countryNumPersons", "countryPairsNumFriends", "creationDayAndLengthCategoryNumMessages", "creationDayAndTagNumMessages", "creationDayAndTagClassNumMessages", "creationDayNumMessages", "languageNumPosts", "lengthNumMessages", "people2Hops", "people4Hops", "personDisjointEmployerPairs", "personNumFriends", "tagClassNumMessages", "tagClassNumTags", "tagNumMessages", "tagNumPersons"]:
    print(f"{entity}")
    parquet_files = [f for f in os.listdir(f"{factor_parquet_path}{entity}/") if f.endswith(".parquet")]
    if not parquet_files:
        raise ValueError(f"No Parquet factor table files found for entity {entity}")
    for parquet_file in parquet_files:
        print(f"- {parquet_file}")
        con.execute(f"DROP TABLE IF EXISTS {entity}")
        con.execute(f"CREATE TABLE {entity} AS SELECT * FROM read_parquet('{factor_parquet_path}{entity}/{parquet_file}')")

print()
print("============ Loading the temporal tables ============")
print("Person_studyAt_University")
parquet_files = [f for f in os.listdir(f"{temporal_parquet_path}Person_studyAt_University/") if f.endswith(".parquet")]
if not parquet_files:
    raise ValueError(f"No Parquet temporal table files found for entity Person_studyAt_University")
for parquet_file in parquet_files:
    print(f"- {parquet_file}")
    con.execute(f"""
        INSERT INTO Person_studyAt_University_window (
            SELECT PersonId, UniversityId
            FROM read_parquet('{temporal_parquet_path}Person_studyAt_University/{parquet_file}')
            WHERE to_timestamp(creationDate/1000) < TIMESTAMP '2012-11-29'
              AND to_timestamp(deletionDate/1000) > TIMESTAMP '2013-01-01'
        );
        """)

print("Person_workAt_Company")
parquet_files = [f for f in os.listdir(f"{temporal_parquet_path}Person_workAt_Company/") if f.endswith(".parquet")]
if not parquet_files:
    raise ValueError(f"No Parquet temporal table files found for entity Person_workAt_Company")
for parquet_file in parquet_files:
    print(f"- {parquet_file}")
    con.execute(f"""
        INSERT INTO Person_workAt_Company_window (
            SELECT personId, companyId
            FROM read_parquet('{temporal_parquet_path}Person_workAt_Company/{parquet_file}')
            WHERE to_timestamp(creationDate/1000) < TIMESTAMP '2012-11-29'
              AND to_timestamp(deletionDate/1000) > TIMESTAMP '2013-01-01'
        );
        """)

print("Person")
parquet_files = [f for f in os.listdir(f"{temporal_parquet_path}Person/") if f.endswith(".parquet")]
if not parquet_files:
    raise ValueError(f"No Parquet temporal table files found for entity Person")
for parquet_file in parquet_files:
    print(f"- {parquet_file}")
    con.execute(f"""
        INSERT INTO person_window (
            SELECT id
            FROM read_parquet('{temporal_parquet_path}Person/{parquet_file}')
            WHERE to_timestamp(creationDate/1000) < TIMESTAMP '2012-11-29'
              AND to_timestamp(deletionDate/1000) > TIMESTAMP '2013-01-01'
        );
        """)

print("Person_knows_Person")
parquet_files = [f for f in os.listdir(f"{temporal_parquet_path}Person_knows_Person/") if f.endswith(".parquet")]
if not parquet_files:
    raise ValueError(f"No Parquet temporal table files found for entity Person_knows_Person")
for parquet_file in parquet_files:
    print(f"- {parquet_file}")
    con.execute(f"""
        INSERT INTO knows_window (
            SELECT person1Id, person2Id
            FROM read_parquet('{temporal_parquet_path}Person_knows_Person/{parquet_file}')
            WHERE to_timestamp(creationDate/1000) < TIMESTAMP '2012-11-29'
              AND to_timestamp(deletionDate/1000) > TIMESTAMP '2013-01-01'
        );
        """)

print()
print("============ Creating materialized views ============")
# d: drop
# m: materialize
for query_variant in ["2d", "2m", "8d", "8m", "20d", "20m"]:
    print(f"- Q{query_variant}")
    with open(f"paramgen-queries/pg-{query_variant}.sql", "r") as parameter_query_file:
        parameter_query = parameter_query_file.read()
        con.execute(parameter_query)

print()
print("============ Generating parameters ============")
for query_variant in ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20"]:
    print(f"- Q{query_variant}")
    with open(f"paramgen-queries/pg-{query_variant}.sql", "r") as parameter_query_file:
        parameter_query = parameter_query_file.read()
        con.execute(f"COPY ( {parameter_query} ) TO '../parameters/bi-{query_variant}.csv' WITH (HEADER, DELIMITER '|');")
