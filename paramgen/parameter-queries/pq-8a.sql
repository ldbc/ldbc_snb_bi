SELECT
    tagName AS 'tag:STRING',
    startDate AS 'startDate:DATE',
    endDate AS 'endDate:DATE'
FROM (
    SELECT
        tagName,
        max(creationDay) - INTERVAL (8 + extract('dayofyear' FROM max(creationDay)) % 7) DAY AS startDate,
        max(creationDay) AS endDate,
        abs(frequency - (SELECT percentile_disc(0.71) WITHIN GROUP (ORDER BY frequency) FROM creationDayAndTagNumMessages)) diff
    FROM creationDayAndTagNumMessages
    GROUP BY diff, tagName
    ORDER BY diff, md5(tagName)
    LIMIT 100
)
