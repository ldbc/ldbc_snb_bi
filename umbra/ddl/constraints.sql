-- the 'contraints.sql' file serves multiple goals:
-- it inserts PK constraints, FK indexes, views, and bidirectional edges

-- bidirectional Person_knows_Person edges
INSERT INTO Person_knows_Person (creationDate, Person1Id, Person2Id)
SELECT creationDate, Person2Id, Person1Id
FROM Person_knows_Person;

-- A recursive materialized view containing the root Post of each Message (for Posts, themselves, for Comments, traversing up the Message thread to the root Post of the tree)
CREATE TABLE Message (
    creationDate timestamp with time zone not null,
    MessageId bigint primary key,
    RootPostId bigint not null,
    RootPostLanguage varchar(40),
    content varchar(2000),
    imageFile varchar(40),
    locationIP varchar(40) not null,
    browserUsed varchar(40) not null,
    length int not null,
    CreatorPersonId bigint not null,
    ContainerForumId bigint,
    LocationCountryId bigint not null,
    ParentMessageId bigint,
    ParentPostId bigint,
    ParentCommentId bigint,
    type varchar(7)
) WITH (storage = paged);

INSERT INTO Message
    WITH RECURSIVE Message_CTE(creationDate, MessageId, RootPostId, RootPostLanguage, content, imageFile, locationIP, browserUsed, length, CreatorPersonId, ContainerForumId, LocationCountryId, ParentMessageId, type) AS (
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
            NULL::bigint AS ParentMessageId,
            NULL::bigint AS ParentPostId,
            NULL::bigint AS ParentCommentId,
            'Post' AS type
        FROM Post
        UNION ALL
        SELECT
            Comment.creationDate AS creationDate,
            Comment.id AS MessageId,
            Message_CTE.RootPostId AS RootPostId,
            Message_CTE.RootPostLanguage AS RootPostLanguage,
            Comment.content AS content,
            NULL::varchar(40) AS imageFile,
            Comment.locationIP AS locationIP,
            Comment.browserUsed AS browserUsed,
            Comment.length AS length,
            Comment.CreatorPersonId AS CreatorPersonId,
            Message_CTE.ContainerForumId AS ContainerForumId,
            Comment.LocationCountryId AS LocationCityId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId,
            Comment.ParentPostId,
            Comment.ParentCommentId,
            'Comment' AS type
        FROM Comment, Message_CTE
        WHERE coalesce(Comment.ParentPostId, Comment.ParentCommentId) = Message_CTE.MessageId
    )
    SELECT * FROM Message_CTE
;

CREATE TABLE Person_likes_Message (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    MessageId bigint NOT NULL
) WITH (storage = paged);

INSERT INTO Person_likes_Message
    SELECT creationDate, PersonId, CommentId AS MessageId FROM Person_likes_Comment
    UNION ALL
    SELECT creationDate, PersonId, PostId AS MessageId FROM Person_likes_Post
;

CREATE TABLE Message_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    MessageId bigint NOT NULL,
    TagId bigint NOT NULL
) WITH (storage = paged);

INSERT INTO Message_hasTag_Tag
    SELECT creationDate, CommentId AS MessageId, TagId FROM Comment_hasTag_Tag
    UNION ALL
    SELECT creationDate, PostId AS MessageId, TagId FROM Post_hasTag_Tag
;

CREATE TABLE Country (
    id bigint primary key,
    name varchar(256) not null,
    url varchar(256) not null,
    PartOfContinentId bigint
) WITH (storage = paged);

INSERT INTO Country
    SELECT id, name, url, PartOfPlaceId AS PartOfContinentId
    FROM Place
    WHERE type = 'Country'
;

CREATE TABLE City (
    id bigint primary key,
    name varchar(256) not null,
    url varchar(256) not null,
    PartOfCountryId bigint
) WITH (storage = paged);

INSERT INTO City
    SELECT id, name, url, PartOfPlaceId AS PartOfCountryId
    FROM Place
    WHERE type = 'City'
;

CREATE TABLE Company (
    id bigint primary key,
    name varchar(256) not null,
    url varchar(256) not null,
    LocationPlaceId bigint not null
) WITH (storage = paged);

INSERT INTO Company
    SELECT id, name, url, LocationPlaceId AS LocatedInCountryId
    FROM Organisation
    WHERE type = 'Company'
;

CREATE TABLE University (
    id bigint primary key,
    name varchar(256) not null,
    url varchar(256) not null,
    LocationPlaceId bigint not null
) WITH (storage = paged);

INSERT INTO University
    SELECT id, name, url, LocationPlaceId AS LocatedInCityId
    FROM Organisation
    WHERE type = 'University'
;

DROP TABLE Post;
DROP TABLE Comment;

CREATE VIEW Comment AS
    SELECT creationDate, MessageId AS id, locationIP, browserUsed, content, length, CreatorPersonId, LocationCountryId, ParentPostId, ParentCommentId
    FROM Message
    WHERE ParentMessageId IS NOT NULL
;

CREATE VIEW Post AS
    SELECT creationDate, MessageId AS id, imageFile, locationIP, browserUsed, RootPostLanguage, content, length, CreatorPersonId, ContainerForumId, LocationCountryId
    From Message
    WHERE ParentMessageId IS NULL
;
