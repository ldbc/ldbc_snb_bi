
DROP TABLE IF EXISTS PathQ20;
CREATE TABLE PathQ20 (
    src bigint not null,
    dst bigint not null,
    w int not null
) with (storage = paged);
INSERT INTO PathQ20(src, dst, w)
select p1.personid, p2.personid, min(abs(p1.classYear - p2.classYear)) + 1
from Person_knows_person pp, Person_studyAt_University p1, Person_studyAt_University p2
where pp.person1id = p1.personid and pp.person2id = p2.personid and p1.universityid = p2.universityid
group by p1.personid, p2.personid;
ALTER TABLE PathQ20 ADD PRIMARY KEY (src, dst);
