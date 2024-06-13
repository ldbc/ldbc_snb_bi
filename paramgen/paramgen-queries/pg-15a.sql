SELECT DISTINCT
    q15a_core.person1Id AS 'person1Id:ID',
    q15a_core.person2Id AS 'person2Id:ID',
    (SELECT date_trunc('day', percentile_disc(0.95) WITHIN GROUP (ORDER BY creationDay)) AS startAnchorDate FROM creationDayNumMessages) - INTERVAL (q15a_core.person1Id % 7) DAY AS 'startDate:DATE',
    (SELECT date_trunc('day', percentile_disc(0.95) WITHIN GROUP (ORDER BY creationDay)) AS endAnchorDate   FROM creationDayNumMessages) + INTERVAL (q15a_core.person2Id % 7) DAY AS 'endDate:DATE'
FROM q15a_core

-- two hops from person2Id
JOIN personKnowsPersonDays_window knows4
  ON knows4.person1Id = q15a_core.person2Id
JOIN personKnowsPersonDays_window knows3
  ON knows3.person1Id = knows4.person2Id

-- meet in the middle
WHERE middleCandidate = knows3.person2Id

ORDER BY md5((131*q15a_core.person1Id + 241*q15a_core.person2Id)::VARCHAR), md5(q15a_core.person1Id::VARCHAR)
