SELECT
      startDate AS 'startDate:DATE',
      endDate AS 'endDate:DATE'
FROM (
      SELECT
            anchorDate::date + INTERVAL (-5 + salt*37 % 10) DAY AS startDate,
            anchorDate::date + INTERVAL (80 + salt*31 % 20) DAY AS endDate
      FROM (
            SELECT percentile_disc(0.89) WITHIN GROUP (ORDER BY creationDay) AS anchorDate
            FROM creationDayNumMessages
      ),
      (SELECT unnest(generate_series(1, 10)) AS salt)
) ORDER BY md5(startDate), md5(endDate)
