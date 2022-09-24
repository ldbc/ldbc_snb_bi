
DROP TABLE IF EXISTS PathQ19;
CREATE TABLE PathQ19 (
    src bigint not null,
    dst bigint not null,
    w double precision not null
) with (storage = paged);
INSERT INTO PathQ19(src, dst, w)
WITH
weights(src, dst, w) as (
    SELECT
        least(m1.creatorpersonid, m2.creatorpersonid) AS src,
        greatest(m1.creatorpersonid, m2.creatorpersonid) AS dst,
        greatest(round(40 - sqrt(count(*)))::bigint, 1) AS w
    from Person_knows_person pp, Message m1, Message m2
    where pp.person1id = m1.creatorpersonid and pp.person2id = m2.creatorpersonid and m1.parentmessageid = m2.messageid and m1.creatorpersonid <> m2.creatorpersonid
    group by src, dst
)
select src, dst, w from weights
union all
select dst, src, w from weights;
ALTER TABLE PathQ19 ADD PRIMARY KEY (src, dst);
