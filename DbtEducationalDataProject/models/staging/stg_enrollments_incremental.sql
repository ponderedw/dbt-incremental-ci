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
    final_grade,
    credits_earned,
    current_timestamp() as updated_at
from {{ source('raw_edu', 'enrollments') }}

{% if is_incremental() %}
    where enrollment_date >= (select max(enrollment_date) from {{ this }})
{% endif %}
