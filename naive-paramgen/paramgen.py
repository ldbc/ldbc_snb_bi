import os
import duckdb

csv_path = "factors/"
con = duckdb.connect(database='factors.duckdb')

print("============ Initializing database ============")
with open(f"paramgen-queries/ddl/schema.sql", "r") as schema_file:
    con.execute(schema_file.read())

print()
print("============ Loading the factor tables ============")
for entity in ["cityNumPersons", "cityPairsNumFriends", "companyNumEmployees", "countryNumMessages", "countryNumPersons", "countryPairsNumFriends", "creationDayAndLengthCategoryNumMessages", "creationDayAndTagNumMessages", "creationDayAndTagClassNumMessages", "creationDayNumMessages", "languageNumPosts", "lengthNumMessages", "people2Hops", "people4Hops", "personDisjointEmployerPairs", "personNumFriends", "tagClassNumMessages", "tagClassNumTags", "tagNumMessages", "tagNumPersons"]:
    print(f"{entity}")
    for csv_file in [f for f in os.listdir(f"{csv_path}{entity}/") if f.endswith(".csv")]:
        print(f"- {csv_file}")
        con.execute(f"COPY {entity} FROM '{csv_path}{entity}/{csv_file}' (FORMAT CSV, DELIMITER ',')")

print()
print("============ Creating materialized views ============")
for query_variant in ["2d", "2m", "8d", "8m"]:
    print(f"- Q{query_variant}")
    with open(f"paramgen-queries/pg-{query_variant}.sql", "r") as parameter_query_file:
        parameter_query = parameter_query_file.read()
        con.execute(parameter_query)

print()
print("============ Generating parameters ============")
for query_variant in ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"]:
    print(f"- Q{query_variant}")
    with open(f"paramgen-queries/pg-{query_variant}.sql", "r") as parameter_query_file:
        parameter_query = parameter_query_file.read()
        con.execute(f"COPY ( {parameter_query} ) TO '../parameters/bi-{query_variant}.csv' WITH (HEADER, DELIMITER '|');")
