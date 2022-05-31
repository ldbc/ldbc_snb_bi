DROP TABLE IF EXISTS Person_UniversityKnows_Person;
DROP TABLE IF EXISTS PersonUniversity;
DROP TABLE IF EXISTS results;

-- PRECOMPUTE
CREATE TEMP TABLE Person_UniversityKnows_Person AS (SELECT p.id                                     as Person1id,
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
CREATE TEMP TABLE PersonUniversity AS (SELECT DISTINCT Person1id as id
                                  FROM ((SELECT Person1id
                                         FROM Person_UniversityKnows_Person)
                                        UNION ALL
                                        (SELECT Person2id AS Person1id
                                         FROM Person_UniversityKnows_Person))
                                  ORDER BY id);

-- CSR CREATION
SELECT DISTINCT CREATE_CSR(
               0,
               v.vcount,
               r.ecount,
               r.src,
               r.dst,
               r.weight
           )
FROM (SELECT count(p.id) as vcount FROM PersonUniversity p) v,
     (SELECT src.rowid as src, dst.rowid as dst, t.weight as weight, count(src.rowid) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as ecount
      FROM Person_UniversityKnows_Person t
       JOIN PersonUniversity src ON t.Person1id = src.id
       JOIN PersonUniversity dst ON t.Person2id = dst.id
      ORDER BY src.rowid, dst.rowid) r;

create temp table results
(
    Person1id bigint,
    Person2id bigint,
    company   varchar,
    weight    bigint
);

-- PRAGMA
pragma set_lane_limit=:param;

-- PRAGMA
pragma threads=:param;


-- PARAMS
INSERT INTO results (SELECT p.id                                                                           as Person1id,
                            p2.id                                                                          as Person2id,
                            c.name                                                                         as Company,
                            cheapest_path(0, (select count(*) from PersonUniversity p), p.rowid, p2.rowid) as weight
                     FROM PersonUniversity p
                              JOIN Person_workAt_Company pwc on p.id = pwc.PersonId
                              JOIN Company c on (pwc.CompanyId = c.id AND c.name = ':company')
                              JOIN PersonUniversity p2 on p2.id = :person2id
                     where weight is not null
                     order by weight, p.id);


-- RESULTS
SELECT (SELECT person1id FROM results WHERE person2id = agg.person2id and company = agg.company and weight = agg.min_weight LIMIT 20)
           as person1id,
        min_weight as weight,
        company,
        person2id

FROM (SELECT min(weight) AS min_weight, person2id, company
      FROM results
      GROUP BY person2id, company) agg
;

