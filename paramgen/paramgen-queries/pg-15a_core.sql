SELECT DISTINCT
    people4Hops_sample.person1Id AS person1Id,
    people4Hops_sample.person2Id AS person2Id,
    knows2.person2Id AS middleCandidate
FROM (
    SELECT *
    FROM people4Hops
    LIMIT 80
) people4Hops_sample
-- two hops from person1Id
JOIN personKnowsPersonDays_window knows1
  ON knows1.person1Id = people4Hops_sample.person1Id
JOIN personKnowsPersonDays_window knows2
  ON knows2.person1Id = knows1.person2Id
