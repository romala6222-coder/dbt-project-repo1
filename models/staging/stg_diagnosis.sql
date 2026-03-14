SELECT
    "Diagnosis ID" as diagnosis_id,
    "DIAGNOSIS" as diagnosis_description
FROM {{ source("raw","diagnosis")}}