SELECT
      anchorDate::date + INTERVAL (- 5 + CAST(FLOOR(10*RANDOM()) AS INT)) DAY  AS 'startDate:DATE',
      anchorDate::date + INTERVAL (80+CAST(FLOOR(20*RANDOM()) AS INT)) DAY AS 'endDate:DATE'
FROM (
     SELECT percentile_disc(0.89) WITHIN GROUP (ORDER BY creationDay) AS anchorDate FROM creationDayNumMessages ),
    (SELECT unnest(generate_series(1, 10)))
