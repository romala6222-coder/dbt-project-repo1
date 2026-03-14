-- models/staging/stg_patients.sql
{{ config(
    materialized='incremental',
    unique_key='patient_sk',
    merge_update_columns=['valid_to', 'is_current'],
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT
        "Patient ID"    AS patient_id,
        "Patient Name"  AS patient_name,
        "GENDER"        AS gender,
        "AGE"::INTEGER  AS age,
        TRY_TO_NUMBER("City ID") AS city_id,
        "RACE"          AS race,
        CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS load_timestamp
    FROM {{ source('raw', 'patience') }}

    -- remove CSV header rows
    WHERE TRY_TO_NUMBER("City ID") IS NOT NULL

    {% if is_incremental() %}
    AND CURRENT_TIMESTAMP() > (SELECT MAX(valid_from) FROM {{ this }})
    {% endif %}
),

-- Generate a hash of all tracked attributes to detect changes
hashed AS (
    SELECT
        *,
        MD5(
            COALESCE(patient_name,'') || '|' ||
            COALESCE(gender,'') || '|' ||
            COALESCE(TO_VARCHAR(age),'') || '|' ||
            COALESCE(TO_VARCHAR(city_id),'') || '|' ||
            COALESCE(race,'')
        ) AS attribute_hash
    FROM source
),

-- In incremental runs, join with existing current records to identify changes
{% if is_incremental() %}
existing AS (
    SELECT
        patient_id,
        attribute_hash,
        valid_from,
        patient_sk
    FROM {{ this }}
    WHERE is_current = TRUE
),
new_records AS (
    SELECT
        h.*,
        e.patient_sk      AS existing_sk,
        e.attribute_hash  AS existing_hash,
        e.valid_from      AS existing_valid_from
    FROM hashed h
    LEFT JOIN existing e
        ON h.patient_id = e.patient_id
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
        'patient_id',
        'load_timestamp'               
    ]) }}     AS patient_sk,
    patient_id,
    patient_name,
    gender,
    age,
    city_id,
    race,
    attribute_hash,
    -- validity dates
    load_timestamp                        AS valid_from,
    CAST(NULL AS TIMESTAMP_NTZ)                AS valid_to,
    TRUE                                   AS is_current
FROM new_records
WHERE
    -- include all records in full refresh
    {% if is_incremental() %}
        -- new patient (no existing)
        existing_sk IS NULL
        -- or existing patient with changed attributes
        OR existing_hash != attribute_hash
    {% else %}
        1=1
    {% endif %}



-- In incremental runs, close out old versions that were replaced
{% if is_incremental() %}
UNION ALL
SELECT
    existing_sk,
    patient_id,
    patient_name,
    gender,
    age,
    city_id,
    race,
    attribute_hash,
    existing_valid_from   AS valid_from,
    load_timestamp         AS valid_to,
    FALSE                  AS is_current
FROM new_records
WHERE existing_hash != attribute_hash
{% endif %}