SELECT
    FIRST(day) AS day,
    status,
    COUNT(status) AS status_count_by_day
FROM (SELECT * FROM mys3.default.http_logs LIMIT 1000)
GROUP BY day, status
ORDER BY day, status
    LIMIT 10;