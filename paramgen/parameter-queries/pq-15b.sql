SELECT
    personNumFriends1.personId AS 'person1Id:ID',
    personNumFriends2.personId AS 'person2Id:ID',
    creationDayNumMessages.creationDay AS 'startDate:DATE',
    creationDayNumMessages.creationDay + 8 + CAST(FLOOR(3*RANDOM()) AS INT) AS 'endDate:DATE'
FROM
    (SELECT * FROM creationDayNumMessages LIMIT 10) creationDayNumMessages,
    (SELECT * FROM personNumFriends LIMIT 10) personNumFriends1,
    (SELECT * FROM personNumFriends LIMIT 10) personNumFriends2
WHERE personNumFriends1.personId != personNumFriends2.personId
LIMIT 400
