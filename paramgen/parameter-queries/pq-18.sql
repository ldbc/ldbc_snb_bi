SELECT
    tagNumMessages.tagName AS 'tag:STRING'
FROM
    (SELECT * FROM tagNumMessages LIMIT 400) tagNumMessages
