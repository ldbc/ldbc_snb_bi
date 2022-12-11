
DROP TABLE IF EXISTS Top100PopularForumsQ04;
CREATE TABLE Top100PopularForumsQ04(
    id bigint not null,
    creationDate timestamp with time zone NOT NULL,
    maxNumberOfMembers bigint not null
) with (storage = paged);
INSERT INTO Top100PopularForumsQ04(id, creationDate, maxNumberOfMembers)
SELECT ForumId AS id, ForumCreationDate as creationDate, max(numberOfMembers) AS maxNumberOfMembers
FROM (
SELECT Forum.id AS ForumId, Forum.creationDate as ForumCreationDate, count(Person.id) AS numberOfMembers, City.PartOfCountryId AS CountryId
    FROM Forum_hasMember_Person
    JOIN Person
    ON Person.id = Forum_hasMember_Person.PersonId
    JOIN City
    ON City.id = Person.LocationCityId
    JOIN Forum
    ON Forum_hasMember_Person.ForumId = Forum.id
    GROUP BY City.PartOfCountryId, Forum.Id, Forum.creationDate
) ForumMembershipPerCountry
GROUP BY ForumId, ForumCreationDate;
ALTER TABLE Top100PopularForumsQ04 ADD PRIMARY KEY (id);
