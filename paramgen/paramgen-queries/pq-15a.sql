SELECT
    person1Id AS 'person1Id:ID',
    person2Id AS 'person2Id:ID',
    (SELECT percentile_disc(0.10) WITHIN GROUP (ORDER BY creationDay) AS startAnchorDate FROM creationDayNumMessages)
        + INTERVAL (-5 + person1Id % 10) DAY
        AS 'startDate:DATE',
    (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY creationDay) AS endAnchorDate   FROM creationDayNumMessages)
        + INTERVAL (-5 + person1Id % 10) DAY
        AS 'endDate:DATE'
FROM people2Hops
ORDER BY md5(131*person1Id + 241*person2Id), md5(person1Id)
LIMIT 400
