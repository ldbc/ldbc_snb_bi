-- delta is set between 8 and 16 hours
SELECT
    tagNumMessages.tagName AS 'tag:STRING',
    8 + CAST(FLOOR(9*RANDOM()) AS INT) AS 'delta:INT'
FROM tagNumMessages
