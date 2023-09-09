SELECT
    person1Id,
    companyId,
    companyName,
FROM (
    SELECT
        personWorkAtCompanyDays_window.personId AS person1Id,
        personWorkAtCompanyDays_window.companyId AS companyId,
        name AS companyName,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.47) WITHIN GROUP (ORDER BY frequency) FROM companyNumEmployees)) AS diff,
        row_number() OVER (PARTITION BY personWorkAtCompanyDays_window.companyId ORDER BY md5(personWorkAtCompanyDays_window.personId)) AS rnum
    FROM companyNumEmployees
    JOIN personWorkAtCompanyDays_window
    ON personWorkAtCompanyDays_window.companyId = companyNumEmployees.id
    ORDER BY diff, md5(personWorkAtCompanyDays_window.personId), md5(personWorkAtCompanyDays_window.companyId)
)
WHERE rnum < 5
LIMIT 200
