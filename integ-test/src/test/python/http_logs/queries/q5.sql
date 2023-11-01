SELECT
    status,
    COUNT(status) AS status_count_by_day
FROM mys3.default.http_logs
WHERE status >= 400
GROUP BY status
ORDER BY status
    LIMIT 20;