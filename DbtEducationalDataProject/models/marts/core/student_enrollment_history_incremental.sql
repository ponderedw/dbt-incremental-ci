{{
    config(
        materialized='incremental',
        unique_key='enrollment_id'
    )
}}

select
    enrollment_id,
    student_id,
    course_id,
    semester_id,
    enrollment_date,
    grade,
    grade_points,
    attendance_percentage,
    -- NEW FIELD ADDED
    case when grade in ('A+', 'A', 'A-') then 'High' else 'Standard' end as performance_level,
    -- ANOTHER NEW FIELD
    case when attendance_percentage >= 90 then 'Excellent' when attendance_percentage >= 75 then 'Good' else 'Poor' end as attendance_rating,
    current_timestamp as last_updated
from {{ ref('stg_enrollments') }}

{% if is_incremental() %}
where enrollment_date > (select coalesce(max(enrollment_date), '1900-01-01'::date) from {{ this }})
{% endif %}
