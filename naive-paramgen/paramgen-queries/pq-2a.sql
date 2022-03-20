SELECT
    date AS 'date:DATE',
    tagClassName AS 'tagClass:STRING'
FROM tagClassAndWindowNumMessages
ORDER BY md5(concat(date, tagClassName))
LIMIT 400
