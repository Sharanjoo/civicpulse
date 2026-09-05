SELECT
    r.category_name AS room_type,  -- Grouping by room type
    CONCAT(r.category_name, ' - ', r.room_name) AS room_type_and_name,  -- Combining room type and room name
    COUNT(*) AS total_bookings,
    ROUND(SUM(f.duration_hours)::numeric, 2) AS total_booked_hours,
    ROUND(AVG(f.duration_hours)::numeric, 2) AS avg_booking_duration_hours
FROM fact_bookings f
JOIN dim_room r
    ON f.room_key = r.room_key
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    r.category_name,
    r.room_name
ORDER BY
    total_bookings desc;