SELECT
    TRY_TO_NUMBER("City ID") AS city_id,
    "CITY" AS city_name,
    "STATE" AS region
FROM {{ source('raw','cities') }}
WHERE TRY_TO_NUMBER("City ID") IS NOT NULL