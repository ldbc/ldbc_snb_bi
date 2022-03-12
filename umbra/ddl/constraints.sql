-- the 'contraints.sql' file serves multiple goals:
-- it inserts PK constraints, FK indexes, views, and bidirectional edges

-- bidirectional Person_knows_Person edges
INSERT INTO Person_knows_Person (creationDate, Person1Id, Person2Id)
SELECT creationDate, Person2Id, Person1Id
FROM Person_knows_Person;

-- Views

CREATE TABLE Message (
    creationDate timestamp with time zone not null,
    id bigint primary key,
    content varchar(2000),
    imageFile varchar(40),
    locationIP varchar(40) not null,
    browserUsed varchar(40) not null,
    language varchar(40),
    length int not null,
    CreatorPersonId bigint not null,
    ContainerForumId bigint,
    LocationCountryId bigint not null,
    ParentMessageId bigint
);

INSERT INTO Message
    SELECT creationDate, id, content, NULL AS imageFile, locationIP, browserUsed, NULL AS language, length, CreatorPersonId, NULL AS ContainerForumId, LocationCountryId, coalesce(ParentPostId, ParentCommentId) AS ParentMessageId
    FROM Comment
    UNION ALL
    SELECT creationDate, id, content, imageFile, locationIP, browserUsed, language, length, CreatorPersonId, ContainerForumId, LocationCountryId, NULL AS ParentMessageId
    FROM Post
;

-- recursive view containing the root Post of each Message (for Posts, themselves, for Comments, traversing up the Message thread to the root Post of the tree)
CREATE TABLE MessageThread (
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
    type varchar(7)
);
INSERT INTO MessageThread
    WITH RECURSIVE MessageThread_CTE(creationDate, MessageId, RootPostId, RootPostLanguage, content, imageFile, locationIP, browserUsed, length, CreatorPersonId, ContainerForumId, LocationCountryId, ParentMessageId, type) AS (
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
            'Post' AS type
        FROM Post
        UNION ALL
        SELECT
            Comment.creationDate AS creationDate,
            Comment.id AS MessageId,
            MessageThread_CTE.RootPostId AS RootPostId,
            MessageThread_CTE.RootPostLanguage AS RootPostLanguage,
            Comment.content AS content,
            NULL::varchar(40) AS imageFile,
            Comment.locationIP AS locationIP,
            Comment.browserUsed AS browserUsed,
            Comment.length AS length,
            Comment.CreatorPersonId AS CreatorPersonId,
            MessageThread_CTE.ContainerForumId AS ContainerForumId,
            Comment.LocationCountryId AS LocationCityId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId,
            'Comment' AS type
        FROM Comment, MessageThread_CTE
        WHERE coalesce(Comment.ParentPostId, Comment.ParentCommentId) = MessageThread_CTE.MessageId
    )
    SELECT * FROM MessageThread_CTE;

CREATE TABLE Person_likes_Message (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    MessageId bigint NOT NULL
);
INSERT INTO Person_likes_Message
    SELECT creationDate, PersonId, CommentId AS MessageId FROM Person_likes_Comment
    UNION ALL
    SELECT creationDate, PersonId, PostId AS MessageId FROM Person_likes_Post
;

CREATE TABLE Message_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    MessageId bigint NOT NULL,
    TagId bigint NOT NULL
);
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
);
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
);
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
);
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
);
INSERT INTO University
    SELECT id, name, url, LocationPlaceId AS LocatedInCityId
    FROM Organisation
    WHERE type = 'University'
;
