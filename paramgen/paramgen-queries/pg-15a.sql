SELECT DISTINCT
    people4Hops_sample.person1Id AS 'person1Id:ID',
    people4Hops_sample.person2Id AS 'person2Id:ID',
    (SELECT date_trunc('day', percentile_disc(0.95) WITHIN GROUP (ORDER BY creationDay)) AS startAnchorDate FROM creationDayNumMessages) - INTERVAL (people4Hops_sample.person1Id % 7) DAY AS 'startDate:DATE',
    (SELECT date_trunc('day', percentile_disc(0.95) WITHIN GROUP (ORDER BY creationDay)) AS endAnchorDate   FROM creationDayNumMessages) + INTERVAL (people4Hops_sample.person2Id % 7) DAY AS 'endDate:DATE'

FROM (
  SELECT *
  FROM people4Hops
  LIMIT 500
) people4Hops_sample

-- two hops from person1Id
JOIN personKnowsPersonDays_window knows1
  ON knows1.person1Id = people4Hops_sample.person1Id
JOIN personKnowsPersonDays_window knows2
  ON knows2.person1Id = knows1.person2Id

-- two hops from person2Id
JOIN personKnowsPersonDays_window knows4
  ON knows4.person1Id = people4Hops_sample.person2Id
JOIN personKnowsPersonDays_window knows3
  ON knows3.person1Id = knows4.person2Id

-- meet in the middle
WHERE knows2.person2Id = knows3.person2Id

ORDER BY md5(131*people4Hops_sample.person1Id + 241*people4Hops_sample.person2Id), md5(people4Hops_sample.person1Id)
LIMIT 400
