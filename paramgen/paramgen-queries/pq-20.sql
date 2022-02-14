SELECT
    comp.companyName AS 'company:STRING',
    pers2.person2Id AS 'person2Id:ID'
FROM
    (SELECT
        companyId,
        companyName,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.47) WITHIN GROUP (ORDER BY frequency) FROM companyNumEmployees)) AS diff
    FROM companyNumEmployees
    ORDER BY diff, companyId
    LIMIT 50) comp,
    (SELECT DISTINCT person2Id FROM PersonDisjointEmployerPairs) pers2
WHERE NOT EXISTS (
        SELECT 1
        FROM PersonDisjointEmployerPairs aj
        WHERE aj.companyId = comp.companyId
          AND aj.person2Id = pers2.person2Id
    )
ORDER BY md5(3532569367*comp.companyId + 211*pers2.person2Id)
LIMIT 400
