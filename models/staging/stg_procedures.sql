SELECT
    "Procedure ID"  as procedure_id,
    "PROCEDURE"     AS procedure_description
FROM    {{ source("raw","procedures")}}