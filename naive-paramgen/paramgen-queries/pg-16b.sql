SELECT
    tagA AS 'tagA:STRING',
    dateA AS 'dateA:DATE',
    tagB AS 'tagB:STRING',
    dateB AS 'dateB:DATE',
    3 + (extract('dayofyear' FROM dateA)+extract('dayofyear' FROM dateB)) % 4 AS 'maxKnowsLimit:INT'
FROM (
    SELECT
        tagDatesA.tagName AS tagA,
        tagDatesA.creationDay AS dateA,
        tagDatesB.tagName AS tagB,
        tagDatesB.creationDay AS dateB
    FROM
        (SELECT creationDay, tagName
         FROM creationDayAndTagNumMessages
         ORDER BY md5(concat(creationDay, tagName))
         LIMIT 100
        ) tagDatesA,
        (SELECT creationDay, tagName
         FROM creationDayAndTagNumMessages
         ORDER BY md5(concat(creationDay, tagName)) DESC
         LIMIT 100
        ) tagDatesB
)
WHERE tagA <> tagB
ORDER BY md5(concat(tagA, tagB)), dateA, dateB
LIMIT 400
