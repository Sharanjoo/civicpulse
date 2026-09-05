SELECT
    DATE_TRUNC('month', d.calendar_date::date) AS month,
    COUNT(*) AS total_bookings,
    ROUND(SUM(f.duration_hours)::numeric, 2) AS total_booked_hours
FROM fact_bookings f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    DATE_TRUNC('month', d.calendar_date::date)
ORDER BY
    month