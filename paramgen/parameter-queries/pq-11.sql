SELECT
    CASE extract('dayofyear' FROM startDate) % 2 == 0 WHEN true THEN 'China' ELSE 'India' END AS country,
    startDate AS 'startDate:DATE',
    endDate AS 'endDate:DATE'
FROM (
      SELECT
            anchorDate::date + INTERVAL (-15 + salt*37 % 30) DAY AS startDate,
            anchorDate::date + INTERVAL (-15 + salt*37 % 30  +  92 + salt*47 % 18) DAY AS endDate
      FROM (
            SELECT percentile_disc(0.92) WITHIN GROUP (ORDER BY creationDay) AS anchorDate
            FROM creationDayNumMessages
      ),
      (SELECT unnest(generate_series(1, 20)) AS salt)
) ORDER BY md5(startDate), md5(endDate)
