-- static tables

CREATE TABLE Organisation (
    id bigint not null PRIMARY KEY,
    type varchar(12) not null,
    name varchar(256) not null,
    url varchar(256) not null,
    isLocatedIn_Place bigint
);

CREATE TABLE Place (
    id bigint not null PRIMARY KEY,
    name varchar(256) not null,
    url varchar(256) not null,
    type varchar(12) not null,
    isPartOf_Place bigint
);

CREATE TABLE Tag (
    id bigint not null PRIMARY KEY,
    name varchar(256) not null,
    url varchar(256) not null,
    hasType_TagClass bigint not null
);

CREATE TABLE TagClass (
    id bigint not null PRIMARY KEY,
    name varchar(256) not null,
    url varchar(256) not null,
    isSubclassOf_TagClass bigint
);

-- static tables / separate table per individual subtype

CREATE TABLE Company (
    id bigint not null PRIMARY KEY,
    name varchar(256) not null,
    url varchar(256) not null,
    isLocatedIn_Country bigint
);

CREATE TABLE University (
    id bigint not null PRIMARY KEY,
    name varchar(256) not null,
    url varchar(256) not null,
    isLocatedIn_City bigint
);

CREATE TABLE Continent (
    id bigint not null PRIMARY KEY,
    name varchar(256) not null,
    url varchar(256) not null
);

CREATE TABLE Country (
    id bigint not null PRIMARY KEY,
    name varchar(256) not null,
    url varchar(256) not null,
    isPartOf_Continent bigint
);

CREATE TABLE City (
    id bigint not null PRIMARY KEY,
    name varchar(256) not null,
    url varchar(256) not null,
    isPartOf_Country bigint
);

-- dynamic tables

CREATE TABLE Comment (
    creationDate timestamp without time zone not null,
    id bigint not null PRIMARY KEY,
    locationIP varchar(40) not null,
    browserUsed varchar(40) not null,
    content varchar(2000) not null,
    length int not null,
    hasCreator_Person bigint not null,
    isLocatedIn_Country bigint not null,
    replyOf_Post bigint,
    replyOf_Comment bigint
);

CREATE TABLE Forum (
    creationDate timestamp without time zone not null,
    id bigint not null PRIMARY KEY,
    title varchar(256) not null,
    hasModerator_Person bigint not null
);
CREATE TABLE Post (
    creationDate timestamp without time zone not null,
    id bigint not null PRIMARY KEY,
    imageFile varchar(40),
    locationIP varchar(40) not null,
    browserUsed varchar(40) not null,
    language varchar(40),
    content varchar(2000),
    length int not null,
    hasCreator_Person bigint not null,
    Forum_containerOf bigint not null,
    isLocatedIn_Country bigint not null
);

CREATE TABLE Person (
    creationDate timestamp without time zone not null,
    id bigint not null PRIMARY KEY,
    firstName varchar(40) not null,
    lastName varchar(40) not null,
    gender varchar(40) not null,
    birthday date not null,
    locationIP varchar(40) not null,
    browserUsed varchar(40) not null,
    isLocatedIn_City bigint not null,
    speaks varchar(640) not null,
    email varchar(8192) not null
);

-- edges
CREATE TABLE Comment_hasTag_Tag        (creationDate timestamp without time zone not null, id bigint not null, hasTag_Tag         bigint not null, PRIMARY KEY (id, hasTag_Tag));
CREATE TABLE Post_hasTag_Tag           (creationDate timestamp without time zone not null, id bigint not null, hasTag_Tag         bigint not null, PRIMARY KEY (id, hasTag_Tag));
CREATE TABLE Forum_hasMember_Person    (creationDate timestamp without time zone not null, id bigint not null, hasMember_Person   bigint not null, PRIMARY KEY (id, hasMember_Person));
CREATE TABLE Forum_hasTag_Tag          (creationDate timestamp without time zone not null, id bigint not null, hasTag_Tag         bigint not null, PRIMARY KEY (id, hasTag_Tag));
CREATE TABLE Person_hasInterest_Tag    (creationDate timestamp without time zone not null, id bigint not null, hasInterest_Tag    bigint not null, PRIMARY KEY (id, hasInterest_Tag));
CREATE TABLE Person_likes_Comment      (creationDate timestamp without time zone not null, id bigint not null, likes_Comment      bigint not null, PRIMARY KEY (id, likes_Comment));
CREATE TABLE Person_likes_Post         (creationDate timestamp without time zone not null, id bigint not null, likes_Post         bigint not null, PRIMARY KEY (id, likes_Post));
CREATE TABLE Person_studyAt_University (creationDate timestamp without time zone not null, id bigint not null, studyAt_University bigint not null, classYear int not null, PRIMARY KEY (id, studyAt_University));
CREATE TABLE Person_workAt_Company     (creationDate timestamp without time zone not null, id bigint not null, workAt_Company     bigint not null, workFrom  int not null, PRIMARY KEY (id, workAt_Company));
CREATE TABLE Person_knows_Person       (creationDate timestamp without time zone not null, Person1id bigint not null, Person2id bigint not null, PRIMARY KEY (Person1id, Person2id));
