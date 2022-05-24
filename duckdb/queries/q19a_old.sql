-- PRECOMPUTE
CREATE TEMP TABLE Message as (SELECT p.id as id, NULL as ParentMessageId, p.CreatorPersonId
                         FROM Post p
                         UNION ALL
                         SELECT c.id                                    as id,
                                coalesce(ParentPostId, ParentCommentId) as ParentMessageId,
                                c.CreatorPersonid
                         FROM comment c);

-- PRECOMPUTE
CREATE TEMP TABLE interactions_old as (SELECT Person_knows_Person.Person1Id AS Person1Id,
                                     Person_knows_Person.Person2Id AS person2Id,
                                     (1.0 / (count(Message1.id)))::DOUBLE AS weight
                              FROM Person_knows_Person
                                       JOIN Message Message1
                                            ON Message1.CreatorPersonId = Person_knows_Person.Person1Id
                                       JOIN Message Message2
                                            ON Message2.CreatorPersonId = Person_knows_Person.Person2Id
                                                AND (Message1.id = Message2.ParentMessageId
                                                    OR Message2.id = Message1.ParentMessageId)
                              GROUP BY Person_knows_Person.Person1Id, Person_knows_Person.Person2Id
                              ORDER BY Person_knows_Person.Person1Id, Person_knows_Person.Person2Id);

-- PRECOMPUTE
CREATE TEMP TABLE PersonInteractions AS (SELECT DISTINCT Person1id as id
                                    FROM ((SELECT Person1id
                                           FROM interactions)
                                          UNION ALL
                                          (SELECT Person2id AS Person1id
                                           FROM interactions))
                                    ORDER BY id);

-- CSR CREATION
SELECT DISTINCT CREATE_CSR(
               0,
               v.vcount,
               r.ecount,
               r.src,
               r.dst,
               r.weight
           ) AS numEdges
FROM (SELECT count(p.id) as vcount FROM PersonInteractions p) v,
     (SELECT src.rowid as src, dst.rowid as dst, t.weight as weight, count(src.rowid) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as ecount
      FROM interactions t
           JOIN PersonInteractions src ON t.Person1id = src.id
           JOIN PersonInteractions dst ON t.Person2id = dst.id) r;


create temp table results
(
    Person1id bigint,
    city1id bigint,
    Person2id bigint,
    city2id bigint,
    weight    double
);


-- PARAMS
INSERT INTO results (SELECT s.id AS person1id, s.LocationCityId AS City1id, s2.id AS person2id, s2.LocationCityId as City2id, cheapest_path(0, (SELECT count(*) FROM PersonInteractions), s.rowid, s2.rowid) AS weight FROM
                (SELECT pi.id, pi.rowid, p.locationcityid FROM personinteractions pi JOIN person p ON p.id = pi.id WHERE p.locationcityid = city1id) s,
                (SELECT pi.id, pi.rowid, p.locationcityid FROM personinteractions pi JOIN person p ON p.id = pi.id WHERE p.locationcityid = city2id) s2
        );

SELECT delete_csr_by_id(0);

-- RESULTS
WITH agg AS (SELECT min(weight) AS min_weight, city1id, city2id
          FROM results
          GROUP BY city1id, city2id
          )
SELECT person1id,
person2id,
weight AS totalWeight,
results.city1id,
results.city2id
FROM results, agg
WHERE results.weight BETWEEN agg.min_weight - 0.00001 AND agg.min_weight + 0.00001 AND
      results.city1id = agg.city1id AND
      results.city2id = agg.city2id
-- ORDER BY totalWeight ASC, person1id ASC, person2id ASC;


