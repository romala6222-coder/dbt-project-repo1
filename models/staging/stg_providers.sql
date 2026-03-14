-- models/staging/stg_providers.sql
{{ config(
    materialized='incremental',
    unique_key='provider_sk',
    merge_update_columns=['valid_to', 'is_current'],
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT
        "Provider ID"     AS provider_id,
        "Provider Name"   AS provider_name,
        "GENDER"          AS gender,
        "NATIONALITY"     AS nationality,
        "AGE"             AS age,
        "IMAGE"           AS image,
        CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS load_timestamp
    FROM {{ source('raw', 'providers') }}
    {% if is_incremental() %}
    -- in a real implementation you'd have a reliable last_updated column;
    -- here we assume the source has no such column, so we always process all
    -- and rely on hash comparison to avoid duplicates.
    -- Alternatively, you could add a filter based on a watermark table.
    {% endif %}
),

-- Generate a hash of all tracked attributes to detect changes
hashed AS (
    SELECT
        *,
        MD5(
            COALESCE(provider_name,'') || '|' ||
            COALESCE(gender,'') || '|' ||
            COALESCE(nationality,'') || '|' ||
            COALESCE(TO_VARCHAR(age),'') || '|' ||
            COALESCE(image,'')
        ) AS attribute_hash
    FROM source
),

-- In incremental runs, join with existing current records to identify changes
{% if is_incremental() %}
existing AS (
    SELECT
        provider_id,
        attribute_hash,
        valid_from,
        provider_sk
    FROM {{ this }}
    WHERE is_current = TRUE
),
new_records AS (
    SELECT
        h.*,
        e.provider_sk      AS existing_sk,
        e.attribute_hash   AS existing_hash,
        e.valid_from       AS existing_valid_from
    FROM hashed h
    LEFT JOIN existing e
        ON h.provider_id = e.provider_id
)
{% else %}
new_records AS (
    SELECT *, NULL AS existing_sk, NULL AS existing_hash, NULL AS existing_valid_from
    FROM hashed
)
{% endif %}

SELECT
    -- generate a new surrogate key for each version
    {{ dbt_utils.generate_surrogate_key([
        'provider_id',
        'load_timestamp'
    ]) }}                                 AS provider_sk,
    provider_id,
    provider_name,
    gender,
    nationality,
    age,
    image,
    attribute_hash,
    load_timestamp                        AS valid_from,
    CAST(NULL AS TIMESTAMP)                AS valid_to,
    TRUE                                   AS is_current
FROM new_records
WHERE
    -- include all records in full refresh
    {% if is_incremental() %}
        -- new provider (no existing)
        existing_sk IS NULL
        -- or existing provider with changed attributes
        OR existing_hash != attribute_hash
    {% else %}
        1=1
    {% endif %}



-- In incremental runs, close out old versions that were replaced
{% if is_incremental() %}

UNION ALL
SELECT
    existing_sk,
    provider_id,
    provider_name,
    gender,
    nationality,
    age,
    image,
    attribute_hash,
    existing_valid_from   AS valid_from,
    load_timestamp         AS valid_to,
    FALSE                  AS is_current
FROM new_records
WHERE existing_hash != attribute_hash
{% endif %}