SELECT
    tagClassName AS 'tagClass:STRING',
    country AS 'country:STRING'
FROM (
    SELECT
        tagClassName,
        abs(frequency - (SELECT percentile_disc(0.82) WITHIN GROUP (ORDER BY frequency)FROM tagClassNumMessages)) AS diff,
        CASE tagClassId % 2 == 0 WHEN true THEN 'China' ELSE 'India' END AS country
    FROM tagClassNumMessages
    ORDER BY diff, tagClassName
    LIMIT 40
)
