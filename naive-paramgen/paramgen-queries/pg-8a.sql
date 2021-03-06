SELECT
    tagName AS 'tag:STRING', 
    startDate AS 'startDate:DATE',
    endDate AS 'endDate:DATE'
FROM tagAndWindowNumMessages
ORDER BY md5(concat(tagName, startDate))
LIMIT 400
