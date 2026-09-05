SELECT
    CASE
        WHEN d.is_exam_period THEN 'Exam Period'
        WHEN d.is_study_day THEN 'Study Day'
        WHEN d.is_recess THEN 'Recess'
        WHEN d.is_holiday THEN 'Holiday'
        WHEN d.is_instruction_day THEN 'Instruction Day'
        ELSE 'Other / Break'
    END AS academic_context,
    COUNT(*) AS total_bookings,
    COUNT(DISTINCT d.calendar_date) AS active_days,
    ROUND(
        COUNT(*)::numeric / NULLIF(COUNT(DISTINCT d.calendar_date), 0),
        2
    ) AS avg_bookings_per_day,
    ROUND(SUM(f.duration_hours)::numeric, 2) AS total_booked_hours
FROM fact_bookings f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY academic_context
ORDER BY total_bookings DESC