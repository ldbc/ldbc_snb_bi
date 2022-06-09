CREATE TEMP TABLE PersonKnows AS (SELECT DISTINCT r.Person1id as id
                                    FROM ((SELECT Person1id
                                            FROM person_knows_person)
                                          UNION ALL
                                          (SELECT Person2id AS Person1id
                                           FROM person_knows_person)) r
                                    ORDER BY id);

-- PRAGMA
pragma set_lane_limit=:param;

-- PRAGMA
pragma threads=:param;

PRAGMA verify_parallelism;

-- CSR CREATION
SELECT CREATE_CSR_VERTEX(
0,
v.vcount,
sub.dense_id,
sub.cnt
) AS numEdges
FROM (
    SELECT c.rowid as dense_id, count(t.person1id) as cnt
    FROM PersonKnows c
    LEFT JOIN  Person_knows_Person t ON t.person1id = c.id
    GROUP BY c.rowid
) sub, (SELECT count(c.id) as vcount FROM PersonKnows c) v;

-- CSR CREATION
SELECT min(CREATE_CSR_EDGE(0, (SELECT count(c.id) as vcount FROM PersonKnows c),
CAST ((SELECT sum(CREATE_CSR_VERTEX(0, (SELECT count(c.id) as vcount FROM PersonKnows c),
sub.dense_id, sub.cnt)) AS numEdges
FROM (
    SELECT c.rowid as dense_id, count(t.person1id) as cnt
    FROM PersonKnows c
    LEFT JOIN  Person_knows_Person t ON t.person1id = c.id
    GROUP BY c.rowid
) sub) AS BIGINT),
src.rowid, dst.rowid))
FROM
  Person_knows_Person t
  JOIN PersonKnows src ON t.person1id = src.id
  JOIN PersonKnows dst ON t.person2id = dst.id;


-- PRECOMPUTE
create temp table results
(
    Person1id bigint,
    Person2id bigint,
    weight bigint
);


-- -- PARAMS
-- INSERT INTO results (SELECT s.id AS person1id, s2.id AS person2id, shortest_path(0, false, (SELECT count(*) FROM PersonKnows), s.rowid, s2.rowid) AS weight FROM
--                 (SELECT p.id, p.rowid FROM PersonKnows p WHERE p.id = :person1id) s,
--                 (SELECT p.id, p.rowid FROM PersonKnows p WHERE p.id = :person2id) s2
-- );


create temp table all_options
(
    Person1id bigint,
    Person1rowid bigint,
    Person2id bigint,
    Person2rowid bigint
);

-- PARAMS
INSERT INTO all_options(
                SELECT s.id AS person1id, s.rowid as person1rowid, s2.id AS person2id, s2.rowid as person2rowid FROM
                (SELECT p.id, p.rowid FROM PersonKnows p WHERE p.id = :person1id) s,
                (SELECT p.id, p.rowid FROM PersonKnows p WHERE p.id = :person2id) s2
);

-- NUMPATHS
select count(*) from all_options;

-- NUMVERTICESEDGES
select num_vertices, num_edges from (select count(*) as num_vertices from PersonKnows),
                                    (select count(*) as num_edges from Person_knows_Person);


-- PATH
INSERT INTO results (SELECT p.person1id, p.person2id, shortest_path(0, true, (select count(*) from PersonKnows), p.person1rowid, p.person2rowid) as weight from all_options p);

pragma delete_csr=0;

-- RESULTS
SELECT weight as totalWeight, person1id, person2id from results;