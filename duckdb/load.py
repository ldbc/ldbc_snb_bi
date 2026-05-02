import duckdb
import psycopg2
import sys
import os
import re
import time
import argparse
from queries import run_script


parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", type=str, help="Directory with the initial_snapshot, insert, and delete directories", required=True)
args = parser.parse_args()
data_dir = args.data_dir

con = duckdb.connect("ldbc.duckdb")

run_script(con, "ddl/drop-tables.sql")
run_script(con, "ddl/schema-composite-merged-fk.sql")
#run_script(pg_con, con, "ddl/schema-delete-candidates.sql")


print("Load initial snapshot")

# initial snapshot
static_path = f"{data_dir}/initial_snapshot/static"
dynamic_path = f"{data_dir}/initial_snapshot/dynamic"
static_entities = ["Organisation", "Place", "Tag", "TagClass"]
dynamic_entities = ["Comment", "Post", "Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag", "Person", "Person_hasInterest_Tag", "Person_knows_Person", "Person_studyAt_University", "Person_workAt_Company", "Comment_hasTag_Tag", "Post_hasTag_Tag", "Person_likes_Comment", "Person_likes_Post"]

print("## Static entities")
for entity in static_entities:
    print(f"- {entity}")
    for csv_file in [f for f in os.listdir(f"{static_path}/{entity}") if f.startswith("part-") and f.endswith(".csv.gz")]:
        print(f"  - {csv_file}")
        con.execute(f"COPY {entity} FROM '{static_path}/{entity}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT csv)")
print("Loaded static entities.")

print("## Dynamic entities")
for entity in dynamic_entities:
    print(f"- {entity}")
    for csv_file in [f for f in os.listdir(f"{dynamic_path}/{entity}") if f.startswith("part-") and f.endswith(".csv.gz")]:
        print(f"  - {csv_file}")
        con.execute(f"COPY {entity} FROM '{dynamic_path}/{entity}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT csv)")
        if entity == "Person_knows_Person":
            con.execute(f"COPY {entity} (creationDate, Person2id, Person1id) FROM '{dynamic_path}/{entity}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT csv)")

print("Create materialized views")

# insert posts to message
con.execute("""
    INSERT INTO Message
    SELECT
        creationDate,
        id AS MessageId,
        id AS RootPostId,
        language AS RootPostLanguage,
        content,
        imageFile,
        locationIP,
        browserUsed,
        length,
        CreatorPersonId,
        ContainerForumId,
        LocationCountryId,
        NULL::bigint AS ParentMessageId
    FROM Post
    """
)

# insert comments to message
con.execute("""
    INSERT INTO Message
        WITH RECURSIVE Message_CTE(MessageId, RootPostId, RootPostLanguage, ContainerForumId, ParentMessageId) AS (
            -- first half of the union: Comments attaching directly to the existing tree
            SELECT
                Comment.id AS MessageId,
                Message.RootPostId AS RootPostId,
                Message.RootPostLanguage AS RootPostLanguage,
                Message.ContainerForumId AS ContainerForumId,
                coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId
            FROM Comment
            JOIN Message
            ON Message.MessageId = coalesce(Comment.ParentPostId, Comment.ParentCommentId)
            UNION ALL
            -- second half of the union: Comments attaching newly inserted Comments
            SELECT
                Comment.id AS MessageId,
                Message_CTE.RootPostId AS RootPostId,
                Message_CTE.RootPostLanguage AS RootPostLanguage,
                Message_CTE.ContainerForumId AS ContainerForumId,
                Comment.ParentCommentId AS ParentMessageId
            FROM Comment
            JOIN Message_CTE
            ON Comment.ParentCommentId = Message_CTE.MessageId
        )
        SELECT
            Comment.creationDate AS creationDate,
            Comment.id AS MessageId,
            Message_CTE.RootPostId AS RootPostId,
            Message_CTE.RootPostLanguage AS RootPostLanguage,
            Comment.content AS content,
            NULL::text AS imageFile,
            Comment.locationIP AS locationIP,
            Comment.browserUsed AS browserUsed,
            Comment.length AS length,
            Comment.CreatorPersonId AS CreatorPersonId,
            Message_CTE.ContainerForumId AS ContainerForumId,
            Comment.LocationCountryId AS LocationCityId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId
        FROM Message_CTE
        JOIN Comment
        ON Message_CTE.MessageId = Comment.id;
    """
)

con.execute("""
    INSERT INTO Message_hasTag_Tag
    SELECT creationDate, PostId, TagId
    FROM Post_hasTag_Tag;
    """)

con.execute("""
    INSERT INTO Message_hasTag_Tag
    SELECT creationDate, CommentId, TagId
    FROM Comment_hasTag_Tag;
    """)

con.execute("""
    INSERT INTO Person_likes_Message
    SELECT creationDate, PersonId, PostId
    FROM Person_likes_Post;
    """)

con.execute("""
    INSERT INTO Person_likes_Message
    SELECT creationDate, PersonId, CommentId
    FROM Person_likes_Comment;
    """)

con.execute("""
    INSERT INTO Country
        SELECT id, name, url, PartOfPlaceId AS PartOfContinentId
        FROM Place
        WHERE type = 'Country'
    ;
    """)

con.execute("""
    INSERT INTO City
        SELECT id, name, url, PartOfPlaceId AS PartOfCountryId
        FROM Place
        WHERE type = 'City'
    ;
    """)

con.execute("""
    INSERT INTO Company
        SELECT id, name, url, LocationPlaceId AS LocatedInCountryId
        FROM Organisation
        WHERE type = 'Company'
    ;
    """)

con.execute("""
    INSERT INTO University
        SELECT id, name, url, LocationPlaceId AS LocatedInCityId
        FROM Organisation
        WHERE type = 'University'
    ;
    """)
