SELECT
    "Insurance ID" as insurance_id,
    "Insurance Provider"as insurance_provider
FROM {{ source("raw","insurance")}}