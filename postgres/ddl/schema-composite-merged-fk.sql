-- static tables

CREATE TABLE Organisation (
    id bigint not null,
    type varchar(12) not null,
    name varchar(256) not null,
    url varchar(256) not null,
    LocationPlaceId bigint not null
);

CREATE TABLE Place (
    id bigint not null,
    name varchar(256) not null,
    url varchar(256) not null,
    type varchar(12) not null,
    PartOfPlaceId bigint -- null for continents
);

CREATE TABLE Tag (
    id bigint not null,
    name varchar(256) not null,
    url varchar(256) not null,
    TypeTagClassId bigint not null
);

CREATE TABLE TagClass (
    id bigint not null,
    name varchar(256) not null,
    url varchar(256) not null,
    SubclassOfTagClassId bigint -- null for the root TagClass (Thing)
);

-- static tables / separate table per individual subtype

-- dynamic tables

CREATE TABLE Comment (
    creationDate timestamp without time zone not null,
    id bigint not null,
    locationIP varchar(40) not null,
    browserUsed varchar(40) not null,
    content varchar(2000) not null,
    length int not null,
    CreatorPersonId bigint not null,
    LocationCountryId bigint not null,
    ParentPostId bigint,
    ParentCommentId bigint
);

CREATE TABLE Forum (
    creationDate timestamp without time zone not null,
    id bigint not null,
    title varchar(256) not null,
    ModeratorPersonId bigint -- can be null as its cardinality is 0..1
);

CREATE TABLE Post (
    creationDate timestamp without time zone not null,
    id bigint not null,
    imageFile varchar(40),
    locationIP varchar(40) not null,
    browserUsed varchar(40) not null,
    language varchar(40),
    content varchar(2000),
    length int not null,
    CreatorPersonId bigint not null,
    ContainerForumId bigint not null,
    LocationCountryId bigint not null
);

CREATE TABLE Person (
    creationDate timestamp without time zone not null,
    id bigint not null,
    firstName varchar(40) not null,
    lastName varchar(40) not null,
    gender varchar(40) not null,
    birthday date not null,
    locationIP varchar(40) not null,
    browserUsed varchar(40) not null,
    LocationCityId bigint not null,
    speaks varchar(640) not null,
    email varchar(8192) not null
);

-- edges
CREATE TABLE Comment_hasTag_Tag        (creationDate timestamp without time zone not null, CommentId bigint not null, TagId bigint not null);
CREATE TABLE Post_hasTag_Tag           (creationDate timestamp without time zone not null, PostId bigint not null,    TagId bigint not null);
CREATE TABLE Forum_hasMember_Person    (creationDate timestamp without time zone not null, ForumId bigint not null,   PersonId bigint not null);
CREATE TABLE Forum_hasTag_Tag          (creationDate timestamp without time zone not null, ForumId bigint not null,   TagId bigint not null);
CREATE TABLE Person_hasInterest_Tag    (creationDate timestamp without time zone not null, PersonId bigint not null,  TagId bigint not null);
CREATE TABLE Person_likes_Comment      (creationDate timestamp without time zone not null, PersonId bigint not null,  CommentId bigint not null);
CREATE TABLE Person_likes_Post         (creationDate timestamp without time zone not null, PersonId bigint not null,  PostId bigint not null);
CREATE TABLE Person_studyAt_University (creationDate timestamp without time zone not null, PersonId bigint not null,  UniversityId bigint not null, classYear int not null);
CREATE TABLE Person_workAt_Company     (creationDate timestamp without time zone not null, PersonId bigint not null,  CompanyId bigint not null, workFrom int not null);
CREATE TABLE Person_knows_Person       (creationDate timestamp without time zone not null, Person1id bigint not null, Person2id bigint not null);
