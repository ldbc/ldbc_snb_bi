DROP TABLE IF EXISTS Person_UniversityKnows_Person;
DROP TABLE IF EXISTS PersonUniversity;
DROP TABLE IF EXISTS src_dst;

CREATE TABLE Person_UniversityKnows_Person AS (
    SELECT p.id as Person1id, p2.id as Person2id, min(abs(u.classYear - u2.classYear) + 1) as weight --
    FROM Person p
    JOIN Person_knows_Person k on p.id = k.Person1id
    JOIN Person p2 on p2.id = k.Person2id
    JOIN Person_studyAt_University u on p.id = u.PersonId
    JOIN Person_studyAt_University u2 on p2.id = u2.PersonId
    WHERE u.UniversityId = u2.UniversityId
    GROUP BY p.id, p2.id
    ORDER BY p.id, p2.id
);




CREATE TABLE PersonUniversity AS (
    SELECT DISTINCT Person1id as id
    FROM (
        (SELECT Person1id
         FROM Person_UniversityKnows_Person)
        UNION ALL
        (SELECT Person2id AS Person1id
         FROM Person_UniversityKnows_Person)
        )
    ORDER BY id
);


SELECT CREATE_CSR_VERTEX(
0,
v.vcount,
sub.dense_id,
sub.cnt
) AS numEdges
FROM (
    SELECT p.rowid as dense_id, count(k.Person1id) as cnt
    FROM PersonUniversity p
    LEFT JOIN  Person_UniversityKnows_Person k ON k.Person1id = p.id
    GROUP BY p.rowid
) sub,  (SELECT count(p.id) as vcount FROM PersonUniversity p) v;

SELECT min(CREATE_CSR_EDGE(0, (SELECT count(p.id) as vcount FROM PersonUniversity p),
CAST ((SELECT sum(CREATE_CSR_VERTEX(0, (SELECT count(p.id) as vcount FROM PersonUniversity p),
sub.dense_id, sub.cnt )) AS numEdges
FROM (
    SELECT p.rowid as dense_id, count(k.Person1id) as cnt
    FROM PersonUniversity p
    LEFT JOIN Person_UniversityKnows_Person k ON k.Person1id = p.id
    GROUP BY p.rowid
) sub) AS BIGINT),
src.rowid, dst.rowid, k.weight))
FROM
  Person_UniversityKnows_Person k
  JOIN PersonUniversity src ON k.Person1id = src.id
  JOIN PersonUniversity dst ON k.Person2id = dst.id;

CREATE TABLE src_dst
    (id integer default 0,
    v_size bigint,
    src bigint,
    dst bigint);

-- PARAMS
INSERT INTO src_dst (src, dst) (SELECT p.rowid, p2.rowid
FROM PersonUniversity p
JOIN Person_workAt_Company pwc on p.id = pwc.PersonId
JOIN Company c on (pwc.CompanyId = c.id AND c.name = 'company')
JOIN PersonUniversity p2 on p2.id = person2);

update src_dst s set v_size = (select count(*) from PersonUniversity p) where v_size is NULL;

select * from src_dst;

SELECT p.id, cheapest_path(s.id, v_size, s.src, s.dst) as weight FROM src_dst s
LEFT JOIN PersonUniversity p ON s.src = p.rowid
LEFT JOIN PersonUniversity p2 ON s.dst = p2.rowid
order by weight, p.id;