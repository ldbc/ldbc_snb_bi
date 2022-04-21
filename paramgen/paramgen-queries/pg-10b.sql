SELECT
    personId AS 'personId:ID',
    countryName AS 'country:STRING',
    tagClassName AS 'tagClass:STRING',
    3 AS 'minPathDistance:INT',
    4 AS 'maxPathDistance:INT'
FROM
    (SELECT
        countryName,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.35) WITHIN GROUP (ORDER BY frequency) FROM countryNumPersons)) AS diff
    FROM countryNumPersons
    ORDER BY diff, countryName
    LIMIT 20),
    (SELECT personNumFriends.personId
    FROM personNumFriends
    JOIN Person_window
      ON Person_window.personId = personNumFriends.personId
    WHERE frequency = 1
    ORDER BY personNumFriends.personId
    LIMIT 50),
    (SELECT
        tagClassName,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.25) WITHIN GROUP (ORDER BY frequency) FROM tagClassNumTags)) AS diff
    FROM tagClassNumTags
    ORDER BY diff, tagClassName
    LIMIT 15)
ORDER BY md5(concat(personId, countryName, tagClassName))
LIMIT 400
