{{
  config(
    materialized = 'table',
    incremental = True,
    unique_key = 'employee_id'
    )
}}

Select
 * , current_timestamp() as processed_at
from {{ source('walmart_source','employees') }}


{% if is_incremental() %}

 WHERE updated_timestamp > (select coalesce(max(updated_timestamp),'1900-01-01') from {{ this }})

{% endif %}
