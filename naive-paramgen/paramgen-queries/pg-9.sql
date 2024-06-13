SELECT
    (creationDay::DATE + INTERVAL (-5 + salt*37 % 10) DAY)::DATE AS 'startDate:DATE',
    (creationDay::DATE + INTERVAL (80 + salt*31 % 20) DAY)::DATE AS 'endDate:DATE'
FROM
    creationDayNumMessages,
    (SELECT unnest(generate_series(1, 20)) AS salt)
