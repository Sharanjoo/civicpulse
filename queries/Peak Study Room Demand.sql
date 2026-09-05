SELECT
    d.day_name,
    d.day_of_week,
    t.hour_24,
    COUNT(*) AS booking_count,
    ROUND(SUM(f.duration_hours)::numeric, 2) AS booked_hours
FROM fact_bookings f
JOIN dim_date d
    ON f.date_key = d.date_key
JOIN dim_time t
    ON f.start_time_key = t.time_key
GROUP BY
    d.day_name,
    d.day_of_week,
    t.hour_24
ORDER BY
    d.day_of_week,
    t.hour_24;