-- variant (b): guaranteed that a path exists
SELECT DISTINCT
    q20b_twohop.companyName AS 'company:STRING',
    q20b_twohop.person2Id AS 'person2Id:ID'
FROM q20b_twohop
-- 'person2' does not work at the company
WHERE NOT EXISTS (SELECT 1
        FROM personWorkAtCompanyDays_window work
        WHERE work.companyId = q20b_twohop.companyId
          AND work.personId = q20b_twohop.person2Id
        )
ORDER BY md5(q20b_twohop.person2Id), md5(q20b_twohop.companyId)
LIMIT 400
