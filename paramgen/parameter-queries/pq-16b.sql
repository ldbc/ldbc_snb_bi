SELECT
    tagNumMessagesA.tagName AS 'tagA:STRING',
    creationDayNumMessagesA.creationDay AS 'dateA:DATE',
    tagNumMessagesB.tagName AS 'tagB:STRING',
    creationDayNumMessagesB.creationDay AS 'dateB:DATE',
    3 + CAST(FLOOR(4*RANDOM()) AS INT) AS 'maxKnowsLimit:INT'
FROM
    (SELECT * FROM tagNumMessages LIMIT 10) tagNumMessagesA,
    (SELECT * FROM creationDayNumMessages LIMIT 10) creationDayNumMessagesA,
    (SELECT * FROM tagNumMessages LIMIT 10) tagNumMessagesB,
    (SELECT * FROM creationDayNumMessages LIMIT 10) creationDayNumMessagesB
WHERE tagNumMessagesA.tagId != tagNumMessagesB.tagId
    AND creationDayNumMessagesA.creationDay != creationDayNumMessagesB.creationDay
LIMIT 400
