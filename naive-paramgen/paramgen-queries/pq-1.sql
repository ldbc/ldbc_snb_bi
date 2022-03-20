SELECT creationDay AS 'datetime:DATETIME'
FROM creationDayNumMessages
ORDER BY md5(creationDay)
LIMIT 400
