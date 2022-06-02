SELECT DISTINCT
    comp.companyName AS 'company:STRING',
    k2.person2Id AS 'person2Id:ID'
FROM
    (SELECT
        person1Id,
        companyId,
        companyName,
      FROM (
          SELECT
              Person_workAt_Company_window.personId AS person1Id,
              Person_workAt_Company_window.companyId AS companyId,
              companyName,
              frequency AS freq,
              abs(frequency - (SELECT percentile_disc(0.47) WITHIN GROUP (ORDER BY frequency) FROM companyNumEmployees)) AS diff,
              row_number() OVER (PARTITION BY Person_workAt_Company_window.companyId ORDER BY md5(Person_workAt_Company_window.personId)) AS rnum
          FROM companyNumEmployees
          JOIN Person_workAt_Company_window
          ON Person_workAt_Company_window.companyId = companyNumEmployees.companyId
          ORDER BY diff, md5(Person_workAt_Company_window.personId), md5(Person_workAt_Company_window.companyId)
      )
      WHERE rnum < 5
      LIMIT 200
    ) comp
-- ensure that there is a two-hop path
-- hop 1
JOIN same_university_knows k1
  ON k1.person1Id = comp.person1Id
-- hop 2
JOIN same_university_knows k2
  ON k2.person1Id = k1.person2Id
 AND k2.person2Id != k1.person1Id
-- 'person2' does not work at the company
WHERE NOT EXISTS (SELECT 1
        FROM Person_workAt_Company_window work
        WHERE work.companyId = comp.companyId
          AND work.personId = k2.person2Id
        )
ORDER BY md5(k2.person2Id), md5(comp.companyId)
LIMIT 400
