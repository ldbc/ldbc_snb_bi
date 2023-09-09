SELECT
    comp.companyId AS companyId,
    comp.companyName AS companyName,
    k2.person2Id AS person2Id
FROM q20b_comp comp
-- ensure that there is a two-hop path
-- hop 1
JOIN q20_sameUniversityKnows k1
  ON k1.person1Id = comp.person1Id
-- hop 2
JOIN q20_sameUniversityKnows k2
  ON k2.person1Id = k1.person2Id
 AND k2.person2Id != k1.person1Id
