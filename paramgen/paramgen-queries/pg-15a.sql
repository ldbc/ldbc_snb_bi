SELECT DISTINCT
    people4Hops.person1Id AS 'person1Id:ID',
    people4Hops.person2Id AS 'person2Id:ID',
    (SELECT date_trunc('day', percentile_disc(0.95) WITHIN GROUP (ORDER BY creationDay)) AS startAnchorDate FROM creationDayNumMessages) - INTERVAL (people4Hops.person1Id % 7) DAY AS 'startDate:DATE',
    (SELECT date_trunc('day', percentile_disc(0.95) WITHIN GROUP (ORDER BY creationDay)) AS endAnchorDate   FROM creationDayNumMessages) + INTERVAL (people4Hops.person2Id % 7) DAY AS 'endDate:DATE'
FROM people4Hops
-- only keep person pairs where both persons exist in the benchmark's time window
JOIN personDays_window p1
  ON p1.id = people4Hops.person1Id
JOIN personDays_window p2
  ON p2.id = people4Hops.person2Id
ORDER BY md5(131*people4Hops.person1Id + 241*people4Hops.person2Id), md5(people4Hops.person1Id)
LIMIT 400
