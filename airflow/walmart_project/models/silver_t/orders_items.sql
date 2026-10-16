{{
  config(
    materialized = 'table',
    incremental = True,
    unique_key = 'order_item_id'
    )
}}

Select
 * , current_timestamp() as processed_at
from {{ source('walmart_source','order_items') }}


{% if is_incremental() %}

  WHERE updated_timestamp > (select coalesce(max(updated_timestamp),'1900-01-01') from {{ this }})

{% endif %}
