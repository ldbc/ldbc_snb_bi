----------------------------------------------------------------------------------------------------
--------------------------------------- APPLY PRECOMP ----------------------------------------------
----------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS Top100PopularForumsQ04;
CREATE TABLE Top100PopularForumsQ04(
    id bigint not null,
    creationDate timestamp with time zone NOT NULL,
    maxNumberOfMembers bigint not null
) with (storage = paged);
INSERT INTO Top100PopularForumsQ04(id, creationDate, maxNumberOfMembers)
SELECT ForumId AS id, ForumCreationDate as creationDate, max(numberOfMembers) AS maxNumberOfMembers
FROM (
SELECT Forum.id AS ForumId, Forum.creationDate as ForumCreationDate, count(Person.id) AS numberOfMembers, Country.id AS CountryId
    FROM Forum_hasMember_Person
    JOIN Person
    ON Person.id = Forum_hasMember_Person.PersonId
    JOIN City
    ON City.id = Person.LocationCityId
    JOIN Country
    ON Country.id = City.PartOfCountryId
    JOIN Forum
    ON Forum_hasMember_Person.ForumId = Forum.id
    GROUP BY Country.Id, Forum.Id, Forum.creationDate
) ForumMembershipPerCountry
GROUP BY ForumId, ForumCreationDate;
ALTER TABLE Top100PopularForumsQ04 ADD PRIMARY KEY (id);


DROP TABLE IF EXISTS PopularityScoreQ06;
CREATE TABLE PopularityScoreQ06 (
    person2id bigint not null,
    popularityScore bigint not null
) with (storage = paged);
INSERT INTO PopularityScoreQ06(person2id, popularityScore)
SELECT
    message2.CreatorPersonId AS person2id,
    count(*) AS popularityScore
FROM Message message2
JOIN Person_likes_Message like2
    ON like2.MessageId = message2.MessageId
GROUP BY message2.CreatorPersonId;
ALTER TABLE PopularityScoreQ06 ADD PRIMARY KEY (person2id);


DROP TABLE IF EXISTS PathQ19;
CREATE TABLE PathQ19 (
    src bigint not null,
    dst bigint not null,
    w double precision not null
) with (storage = paged);
INSERT INTO PathQ19(src, dst, w)
WITH
weights(src, dst, c) as (
    select least(m1.creatorpersonid, m2.creatorpersonid) as src,
           greatest(m1.creatorpersonid, m2.creatorpersonid) as dst,
           count(*) as c
    from Person_knows_person pp, Message m1, Message m2
    where pp.person1id = m1.creatorpersonid and pp.person2id = m2.creatorpersonid and m1.parentmessageid = m2.messageid and m1.creatorpersonid <> m2.creatorpersonid
    group by src, dst
)
select src, dst, 1.0::double precision / c from weights
union all
select dst, src, 1.0::double precision / c from weights;
ALTER TABLE PathQ19 ADD PRIMARY KEY (src, dst);


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
