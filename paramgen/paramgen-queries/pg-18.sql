SELECT
    tagName AS 'tag:STRING'
FROM (
    SELECT
        tagName,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY frequency) FROM tagNumPersons)) AS diff
    FROM tagNumPersons
    ORDER BY diff, tagName
    LIMIT 400
)
