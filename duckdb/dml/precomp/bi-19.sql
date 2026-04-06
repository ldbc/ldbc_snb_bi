
DROP TABLE IF EXISTS PathQ19;
CREATE TABLE PathQ19 (
    src bigint not null,
    dst bigint not null,
    w double precision not null
) with (storage = paged);
INSERT INTO PathQ19(src, dst, w)
WITH
weights(src, dst, w) AS (
    SELECT
        person1id AS src,
        person2id AS dst,
        greatest(round(40 - sqrt(count(*)))::bigint, 1) AS w
    FROM (SELECT person1id, person2id FROM Person_knows_person WHERE person1id < person2id) pp, Message m1, Message m2
    WHERE pp.person1id = least(m1.creatorpersonid, m2.creatorpersonid) and pp.person2id = greatest(m1.creatorpersonid, m2.creatorpersonid) and m1.parentmessageid = m2.messageid and m1.creatorpersonid <> m2.creatorpersonid
    GROUP BY src, dst
)
SELECT src, dst, w FROM weights
UNION ALL
SELECT dst, src, w FROM weights;
ALTER TABLE PathQ19 ADD PRIMARY KEY (src, dst);
