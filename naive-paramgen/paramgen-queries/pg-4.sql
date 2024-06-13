SELECT creationDay AS 'date:DATE'
FROM creationDayNumMessages
ORDER BY md5(creationDay::VARCHAR)
