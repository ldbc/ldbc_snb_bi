SELECT
    strftime(creationDay, '%Y-%m-%dT%H:%M:%S.%g+00:00') AS 'datetime:DATETIME'
FROM creationDayNumMessages
LIMIT 400
