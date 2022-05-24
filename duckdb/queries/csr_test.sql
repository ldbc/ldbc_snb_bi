DROP TABLE IF EXISTS results;
DROP TABLE IF EXISTS person_universityknows_person;
DROP TABLE IF EXISTS PersonUniversity;

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
       JOIN PersonUniversity dst ON t.Person2id = dst.id) r;