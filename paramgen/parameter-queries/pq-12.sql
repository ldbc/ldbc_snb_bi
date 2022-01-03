-- Q12
SELECT
    creationDayNumMessages.creationDay AS 'date:DATE',
    lengthNumMessages.length AS 'lengthThreshold:INT',
    languageNumPosts.language AS 'languages:STRING[]' -- should be multiple languages concatenated to a single string
FROM
    (SELECT creationDay FROM creationDayNumMessages LIMIT 10) creationDayNumMessages,
    (SELECT length FROM lengthNumMessages LIMIT 10) lengthNumMessages, -- OFFSET count/5?
    (SELECT language FROM languageNumPosts LIMIT 10) languageNumPosts
