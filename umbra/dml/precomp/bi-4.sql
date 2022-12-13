
DROP TABLE IF EXISTS Top100PopularForumsQ04;
CREATE TABLE Top100PopularForumsQ04(
    id bigint not null,
    creationDate timestamp with time zone NOT NULL,
    maxNumberOfMembers bigint not null
) with (storage = paged);
INSERT INTO Top100PopularForumsQ04(id, creationDate, maxNumberOfMembers)
SELECT T.id, Forum.creationdate, T.maxNumberOfMembers
FROM (SELECT ForumId AS id, max(numberOfMembers) AS maxNumberOfMembers
FROM (
SELECT Forum_hasMember_Person.ForumId AS ForumId, count(Person.id) AS numberOfMembers, City.PartOfCountryId AS CountryId
    FROM Forum_hasMember_Person
    JOIN Person
    ON Person.id = Forum_hasMember_Person.PersonId
    JOIN City
    ON City.id = Person.LocationCityId
    GROUP BY City.PartOfCountryId, Forum_hasMember_Person.ForumId
) ForumMembershipPerCountry
GROUP BY ForumId) T, Forum
WHERE T.id = Forum.id;
ALTER TABLE Top100PopularForumsQ04 ADD PRIMARY KEY (id);
