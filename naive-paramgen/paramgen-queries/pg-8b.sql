SELECT
    name AS 'tag:STRING', 
    startDate AS 'startDate:DATE',
    endDate AS 'endDate:DATE'
FROM tagAndWindowNumMessages
ORDER BY md5(concat(name, startDate))
LIMIT 400
