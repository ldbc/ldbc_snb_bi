CREATE TEMP TABLE PersonKnows AS (SELECT DISTINCT r.Person1id as id
                                    FROM ((SELECT Person1id
                                           FROM person_knows_person)
                                          UNION ALL
                                          (SELECT Person2id AS Person1id
                                           FROM person_knows_person)) r
                                    ORDER BY id);


-- CSR CREATION
SELECT DISTINCT CREATE_CSR(
               0,
               v.vcount,
               r.ecount,
               r.src,
               r.dst
           )
FROM (SELECT count(p.id) as vcount FROM PersonKnows p) v,
     (SELECT src.rowid as src, dst.rowid as dst, count(src.rowid) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as ecount
      FROM Person_knows_Person t
       JOIN PersonKnows src ON t.Person1id = src.id
       JOIN PersonKnows dst ON t.Person2id = dst.id
       order by src.rowid, dst.rowid) r;

-- PRECOMPUTE
create temp table results
(
    Person1id bigint,
    Person2id bigint,
    weight bigint
);


-- PARAMS
INSERT INTO results (SELECT s.id AS person1id, s2.id AS person2id, shortest_path(0, false, (SELECT count(*) FROM PersonKnows), s.rowid, s2.rowid) AS weight FROM
                (SELECT p.id, p.rowid FROM PersonKnows p WHERE p.id = :person1id) s,
                (SELECT p.id, p.rowid FROM PersonKnows p WHERE p.id = :person2id) s2
);

INSERT INTO results (SELECT s.id AS person1id, s2.id AS person2id, shortest_path(0, false, (SELECT count(*) FROM PersonKnows), s.rowid, s2.rowid) AS weight FROM
                (SELECT p.id, p.rowid FROM PersonKnows p WHERE p.id = 15393162790310) s,
                (SELECT p.id, p.rowid FROM PersonKnows p WHERE p.id = 2199023255851) s2
);

-- RESULTS
SELECT weight as totalWeight, person1id, person2id from results;