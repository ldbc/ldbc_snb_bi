SELECT
    creationDay AS 'date:DATE',
    tagClassName AS 'tagClass:STRING'
FROM (
    SELECT
        creationDay,
        tagClassName,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.60) WITHIN GROUP (ORDER BY frequency) FROM creationDayAndTagClassNumMessages)) AS diff
    FROM creationDayAndTagClassNumMessages
    ORDER BY diff, creationDay
    LIMIT 400
)
