SELECT
    CASE extract('dayofyear' FROM endDate) % 2 == 0 WHEN true THEN 'India' ELSE 'China' END AS 'country:STRING',
    endDate AS 'endDate:DATE'
FROM (
    SELECT DISTINCT
        anchorDate::date + INTERVAL (-5 + salt*47 % 12) DAY AS endDate
    FROM (
        SELECT percentile_disc(0.94) WITHIN GROUP (ORDER BY creationDay) AS anchorDate
        FROM creationDayNumMessages
    ),
    (SELECT unnest(generate_series(456789, 456789+200)) AS salt)
)
ORDER BY md5(endDate)
