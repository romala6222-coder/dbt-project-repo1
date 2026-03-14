SELECT
    "Department ID" as department_id,
    "DEPARTMENT"    as department_name
FROM {{ source("raw","departments")}}