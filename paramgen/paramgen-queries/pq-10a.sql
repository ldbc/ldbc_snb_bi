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
        abs(frequency - (SELECT percentile_disc(0.55) WITHIN GROUP (ORDER BY frequency) FROM countryNumPersons)) AS diff
    FROM countryNumPersons
    ORDER BY diff, countryName
    LIMIT 20),
    (SELECT
        personId,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.55) WITHIN GROUP (ORDER BY frequency) FROM personNumFriends)) AS diff
    FROM personNumFriends
    ORDER BY diff, personId
    LIMIT 50),
    (SELECT
        tagClassName,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.55) WITHIN GROUP (ORDER BY frequency) FROM tagClassNumTags)) AS diff
    FROM tagClassNumTags
    ORDER BY diff, tagClassName
    LIMIT 15)
ORDER BY md5(concat(personId, countryName, tagClassName))
LIMIT 400
