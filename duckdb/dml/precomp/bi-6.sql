
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
