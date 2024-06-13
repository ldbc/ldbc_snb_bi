SELECT creationDay AS 'date:DATE'
FROM (
    SELECT creationDay::DATE AS creationDay
    FROM creationDayNumMessages
    ORDER BY creationDay ASC
    LIMIT 40
)
ORDER BY md5(creationDay::VARCHAR)
