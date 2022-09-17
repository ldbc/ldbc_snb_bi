-- variant (a): guaranteed that no path exists
-- we use the following algorithm:
-- 1) we first select 20 of companies
-- 2) we list *all* their employees (person1 candidates)
-- 3) for each company, we select person2s who do not have a path to *any* of the person1 candidates
SELECT
    s1.companyName AS 'company:STRING',
    s2.personId AS 'person2Id:ID'
FROM
    (SELECT companyId, companyName
    FROM
        (SELECT
            id AS companyId,
            name AS companyName,
            frequency AS freq,
            abs(frequency - (SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY frequency) FROM companyNumEmployees)) AS diff,
        FROM companyNumEmployees
        ORDER BY diff
        )
    ) s1,
    (SELECT
        personNumFriends.id AS personId,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.55) WITHIN GROUP (ORDER BY frequency) FROM personNumFriends)) AS diff
    FROM personNumFriends
    JOIN personDays_window
      ON personDays_window.id = personNumFriends.id
    ORDER BY diff, md5(personNumFriends.id)
    LIMIT 50
    ) s2
WHERE NOT EXISTS (
        SELECT 1
        FROM sameUniversityConnected c1, sameUniversityConnected c2, personWorkAtCompanyDays_window
        WHERE c1.PersonId = s2.personId
          AND personWorkAtCompanyDays_window.PersonId = c2.personId
          AND personWorkAtCompanyDays_window.companyId = s1.companyId
          AND c1.Component = c2.Component
    )
ORDER BY md5(s2.personId), md5(s1.companyId)
LIMIT 400
