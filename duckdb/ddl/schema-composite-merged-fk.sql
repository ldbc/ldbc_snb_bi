-- static tables

CREATE TABLE Organisation (
    id bigint PRIMARY KEY,
    type varchar(12) NOT NULL,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    LocationPlaceId bigint NOT NULL
);

CREATE TABLE Person (
    creationDate timestamp NOT NULL,
    id bigint PRIMARY KEY,
    firstName varchar(40) NOT NULL,
    lastName varchar(40) NOT NULL,
    gender varchar(40) NOT NULL,
    birthday date NOT NULL,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks varchar(640) NOT NULL,
    email varchar(8192) NOT NULL
);

CREATE TABLE Person_studyAt_University (
    creationDate timestamp NOT NULL,
    PersonId bigint NOT NULL,
    UniversityId bigint NOT NULL,
    classYear int NOT NULL
    --, PRIMARY KEY(PersonId, UniversityId)
);

CREATE TABLE Person_workAt_Company (
    creationDate timestamp NOT NULL,
    PersonId bigint NOT NULL,
    CompanyId bigint NOT NULL,
    workFrom int NOT NULL
    --, PRIMARY KEY(PersonId, CompanyId)
);

CREATE TABLE Person_knows_Person (
    creationDate timestamp NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
    --, PRIMARY KEY(Person1id, Person2id)
);

CREATE VIEW Company AS
	SELECT id, name, url, LocationPlaceId
	FROM Organisation
	WHERE type = 'Company'
;

CREATE VIEW University AS
	SELECT id, name, url, LocationPlaceId
	FROM Organisation
	WHERE type = 'University'
;

copy Organisation from '/home/daniel/Documents/Programming/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-merged-fk/initial_snapshot/static/Organisation/part-00000-c885b73f-7f2e-4109-9acd-65fabee6c16a-c000.csv' (DELIMITER '|', HEADER);

copy Person from '/home/daniel/Documents/Programming/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic/Person/part-00000-354b330a-be7e-4581-99ab-dfe73df59470-c000.csv' (DELIMITER '|', HEADER);
copy Person from '/home/daniel/Documents/Programming/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic/Person/part-00001-354b330a-be7e-4581-99ab-dfe73df59470-c000.csv' (DELIMITER '|', HEADER);

copy Person_knows_Person from '/home/daniel/Documents/Programming/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic/Person_knows_Person/part-00000-97235d24-1e63-44da-a16e-fbec913d8097-c000.csv' (DELIMITER '|', HEADER);
copy Person_knows_Person from '/home/daniel/Documents/Programming/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic/Person_knows_Person/part-00001-97235d24-1e63-44da-a16e-fbec913d8097-c000.csv' (DELIMITER '|', HEADER);

copy Person_workAt_Company from '/home/daniel/Documents/Programming/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic/Person_workAt_Company/part-00000-2c95aeeb-87c8-470d-8438-3b6ad49a5131-c000.csv' (DELIMITER '|', HEADER);
copy Person_workAt_Company from '/home/daniel/Documents/Programming/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic/Person_workAt_Company/part-00001-2c95aeeb-87c8-470d-8438-3b6ad49a5131-c000.csv' (DELIMITER '|', HEADER);

copy Person_studyAt_University from '/home/daniel/Documents/Programming/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic/Person_studyAt_University/part-00000-a7960d43-e786-49e2-b86e-f47edafde7fb-c000.csv' (DELIMITER '|', HEADER);
copy Person_studyAt_University from '/home/daniel/Documents/Programming/ldbc_snb_datagen_spark/out-sf1/graphs/csv/bi/composite-merged-fk/initial_snapshot/dynamic/Person_studyAt_University/part-00001-a7960d43-e786-49e2-b86e-f47edafde7fb-c000.csv' (DELIMITER '|', HEADER);


insert into Person_knows_Person select creationdate, person2id, person1id from Person_knows_Person;

SELECT CREATE_CSR_VERTEX(
0,
v.vcount,
sub.dense_id,
sub.cnt
) AS numEdges
FROM (
    SELECT p.rowid as dense_id, count(k.Person1id) as cnt
    FROM Person p
    LEFT JOIN  Person_knows_Person k ON k.Person1id = p.id
    GROUP BY p.rowid
) sub,  (SELECT count(p.id) as vcount FROM Person p) v;

SELECT min(CREATE_CSR_EDGE(0, (SELECT count(c.cid) as vcount FROM Customer c),
CAST ((SELECT sum(CREATE_CSR_VERTEX(0, (SELECT count(c.cid) as vcount FROM Customer c),
sub.dense_id , sub.cnt )) AS numEdges
FROM (
    SELECT c.rowid as dense_id, count(t.from_id) as cnt
    FROM Customer c
    LEFT JOIN  Transfers t ON t.from_id = c.cid
    GROUP BY c.rowid
) sub) AS BIGINT),
src.rowid, dst.rowid))
FROM
  Transfers t
  JOIN Customer src ON t.from_id = src.cid
  JOIN Customer dst ON t.to_id = dst.cid


SELECT min(CREATE_CSR_EDGE(0, (SELECT count(p.id) as vcount FROM Person p),
CAST ((SELECT sum(CREATE_CSR_VERTEX(0, (SELECT count(p.id) as vcount FROM Person p),
sub.dense_id , sub.cnt )) AS numEdges
FROM (
    SELECT p.rowid as dense_id, count(k.Person1id) as cnt
    FROM Person p
    LEFT JOIN Person_knows_Person k ON k.Person1id = p.id
    GROUP BY p.rowid
) sub) AS BIGINT),
src.rowid, dst.rowid))
FROM
  Person_knows_Person k
  JOIN Person src ON k.Person1id = src.id
  JOIN Person dst ON k.Person2id = dst.id;

CREATE TABLE src_dest(id int, v_size bigint, src bigint, dst bigint);


-- CREATE TABLE Person_UniversityKnows_Person AS (
    SELECT p.id as p1id, p2.id as p2id, min(abs(u.classYear - u2.classYear) + 1) as weight --
    FROM Person p
    JOIN Person_knows_Person k on p.id = k.Person1id
    JOIN Person p2 on p2.id = k.Person2id
    JOIN Person_studyAt_University u on p.id = u.PersonId
    JOIN Person_studyAt_University u2 on p2.id = u2.PersonId
    WHERE u.UniversityId = u2.UniversityId
    GROUP BY p.id, p2.id
--     );

