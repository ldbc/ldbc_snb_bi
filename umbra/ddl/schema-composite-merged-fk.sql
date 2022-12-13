-- static tables

CREATE TABLE Organisation (
    id bigint PRIMARY KEY,
    type text NOT NULL,
    name text NOT NULL,
    url text NOT NULL,
    LocationPlaceId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Place (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    type text NOT NULL,
    PartOfPlaceId bigint -- null for continents
) WITH (storage = paged);

CREATE TABLE Tag (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    TypeTagClassId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE TagClass (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    SubclassOfTagClassId bigint -- null for the root TagClass (Thing)
) WITH (storage = paged);

-- static tables / separate table per individual subtype

CREATE TABLE Country (
    id bigint primary key,
    name text not null,
    url text not null,
    PartOfContinentId bigint
) WITH (storage = paged);

CREATE TABLE City (
    id bigint primary key,
    name text not null,
    url text not null,
    PartOfCountryId bigint
) WITH (storage = paged);

CREATE TABLE Company (
    id bigint primary key,
    name text not null,
    url text not null,
    LocationPlaceId bigint not null
) WITH (storage = paged);

CREATE TABLE University (
    id bigint primary key,
    name text not null,
    url text not null,
    LocationPlaceId bigint not null
) WITH (storage = paged);

-- dynamic tables

CREATE TABLE Comment (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL, --PRIMARY KEY,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    content text NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    ParentPostId bigint,
    ParentCommentId bigint
) WITH (storage = paged);

CREATE TABLE Forum (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    title text NOT NULL,
    ModeratorPersonId bigint -- can be null as its cardinality is 0..1
) WITH (storage = paged);

CREATE TABLE Post (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL, --PRIMARY KEY,
    imageFile text,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    language text,
    content text,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint NOT NULL,
    LocationCountryId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Person (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    firstName text NOT NULL,
    lastName text NOT NULL,
    gender text NOT NULL,
    birthday date NOT NULL,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks text NOT NULL,
    email text NOT NULL
) WITH (storage = paged);


-- edges
CREATE TABLE Comment_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    CommentId bigint NOT NULL,
    TagId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Post_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    PostId bigint NOT NULL,
    TagId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Forum_hasMember_Person (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Forum_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    TagId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Person_hasInterest_Tag (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    TagId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Person_likes_Comment (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Person_likes_Post (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Person_studyAt_University (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    UniversityId bigint NOT NULL,
    classYear int NOT NULL
) WITH (storage = paged);

CREATE TABLE Person_workAt_Company (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CompanyId bigint NOT NULL,
    workFrom int NOT NULL
) WITH (storage = paged);

CREATE TABLE Person_knows_Person (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL,
    PRIMARY KEY (Person1id, Person2id)
) WITH (storage = paged);


-- materialized views

-- A recursive materialized view containing the root Post of each Message (for Posts, themselves, for Comments, traversing up the Message thread to the root Post of the tree)
CREATE TABLE Message (
    creationDate timestamp with time zone not null,
    MessageId bigint primary key,
    RootPostId bigint not null,
    RootPostLanguage text,
    content text,
    imageFile text,
    locationIP text not null,
    browserUsed text not null,
    length int not null,
    CreatorPersonId bigint not null,
    ContainerForumId bigint,
    LocationCountryId bigint not null,
    ParentMessageId bigint
) WITH (storage = paged);

CREATE TABLE Person_likes_Message (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    MessageId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Message_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    MessageId bigint NOT NULL,
    TagId bigint NOT NULL
) WITH (storage = paged);

-- views

CREATE VIEW Comment_View AS
    SELECT creationDate, MessageId AS id, locationIP, browserUsed, content, length, CreatorPersonId, LocationCountryId, ParentMessageId
    FROM Message
    WHERE ParentMessageId IS NOT NULL;

CREATE VIEW Post_View AS
    SELECT creationDate, MessageId AS id, imageFile, locationIP, browserUsed, RootPostLanguage, content, length, CreatorPersonId, ContainerForumId, LocationCountryId
    From Message
    WHERE ParentMessageId IS NULL;
