{% snapshot customers_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',

        strategy='timestamp',

        updated_at='updated_at',

        invalidate_hard_deletes=True,

        tags=['snapshot','scd2']
    )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    status,
    updated_at
FROM {{ source('crm', 'customers') }}

{% endsnapshot %}