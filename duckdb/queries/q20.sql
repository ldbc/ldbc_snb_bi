DROP TABLE IF EXISTS Person_UniversityKnows_Person;
DROP TABLE IF EXISTS PersonUniversity;
DROP TABLE IF EXISTS results;

-- PRECOMPUTE
CREATE TABLE Person_UniversityKnows_Person AS (SELECT p.id                                     as Person1id,
                                                      p2.id                                    as Person2id,
                                                      min(abs(u.classYear - u2.classYear) + 1) as weight --
                                               FROM Person p
                                                        JOIN Person_knows_Person k on p.id = k.Person1id
                                                        JOIN Person p2 on p2.id = k.Person2id
                                                        JOIN Person_studyAt_University u on p.id = u.PersonId
                                                        JOIN Person_studyAt_University u2 on p2.id = u2.PersonId
                                               WHERE u.UniversityId = u2.UniversityId
                                               GROUP BY p.id, p2.id
                                               ORDER BY p.id, p2.id);


-- PRECOMPUTE
CREATE TABLE PersonUniversity AS (SELECT DISTINCT Person1id as id
                                  FROM ((SELECT Person1id
                                         FROM Person_UniversityKnows_Person)
                                        UNION ALL
                                        (SELECT Person2id AS Person1id
                                         FROM Person_UniversityKnows_Person))
                                  ORDER BY id);

-- CSR CREATION
SELECT CREATE_CSR_VERTEX(
               0,
               v.vcount,
               sub.dense_id,
               sub.cnt
           ) AS numEdges
FROM (SELECT p.rowid as dense_id, count(k.Person1id) as cnt
      FROM PersonUniversity p
               LEFT JOIN Person_UniversityKnows_Person k ON k.Person1id = p.id
      GROUP BY p.rowid) sub,
     (SELECT count(p.id) as vcount FROM PersonUniversity p) v;

-- CSR CREATION
SELECT min(CREATE_CSR_EDGE(0, (SELECT count(p.id) as vcount FROM PersonUniversity p),
                           CAST((SELECT sum(CREATE_CSR_VERTEX(0, (SELECT count(p.id) as vcount FROM PersonUniversity p),
                                                              sub.dense_id, sub.cnt)) AS numEdges
                                 FROM (SELECT p.rowid as dense_id, count(k.Person1id) as cnt
                                       FROM PersonUniversity p
                                                LEFT JOIN Person_UniversityKnows_Person k ON k.Person1id = p.id
                                       GROUP BY p.rowid) sub) AS BIGINT),
                           src.rowid, dst.rowid, k.weight))
FROM Person_UniversityKnows_Person k
         JOIN PersonUniversity src ON k.Person1id = src.id
         JOIN PersonUniversity dst ON k.Person2id = dst.id;

create table results
(
    Person1id bigint,
    Person2id bigint,
    company   varchar,
    weight    bigint
);

-- PARAMS
INSERT INTO results (SELECT p.id                                                                           as Person1id,
                            p2.id                                                                          as Person2id,
                            c.name                                                                         as Company,
                            cheapest_path(0, (select count(*) from PersonUniversity p), p.rowid, p2.rowid) as weight
                     FROM PersonUniversity p
                              JOIN Person_workAt_Company pwc on p.id = pwc.PersonId
                              JOIN Company c on (pwc.CompanyId = c.id AND c.name = 'company')
                              JOIN PersonUniversity p2 on p2.id = person2id
                     where weight is not null
                     order by weight, p.id);


-- RESULTS
SELECT (SELECT person1id FROM results WHERE person2id = agg.person2id and company = agg.company and weight = agg.min_weight LIMIT 20)
           as person1id,
       person2id,
       company,
       min_weight as weight
FROM (SELECT min(weight) AS min_weight, person2id, company
      FROM results
      GROUP BY person2id, company) agg
;

