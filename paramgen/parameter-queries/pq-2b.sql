SELECT creationDay AS 'date:DATE', tagClassName AS 'tagClass:STRING' FROM ( SELECT
    creationDayAndTagClassNumMessages.creationDay,
    creationDayAndTagClassNumMessages.tagClassName,
    creationDayAndTagClassNumMessages.frequency AS freq,
    abs(creationDayAndTagClassNumMessages.frequency -  (
      SELECT percentile_disc(0.25) WITHIN GROUP (ORDER BY frequency) FROM creationDayAndTagClassNumMessages
    )  ) AS diff
FROM
   creationDayAndTagClassNumMessages
ORDER BY diff
LIMIT 400)
