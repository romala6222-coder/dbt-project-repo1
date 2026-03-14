{{ config(
    materialized='incremental',
    unique_key='visit_id',
    on_schema_change='sync_all_columns'
) }}

SELECT

    {{ dbt_utils.generate_surrogate_key([
        '"Patient ID"',
        '"Provider ID"',
        '"Date of Visit"',
        '"Department ID"',
        '"Procedure ID"'
    ]) }} AS visit_id,

    "Date of Visit"              AS visit_date,
    "Patient ID"                 AS patient_id,
    "Provider ID"                AS provider_id,
    "Department ID"              AS department_id,
    "Diagnosis ID"               AS diagnosis_id,
    "Procedure ID"               AS procedure_id,
    "Insurance ID"               AS insurance_id,
    "Service Type"               AS service_type,
    "Treatment Cost"             AS treatment_cost,
    "Medication Cost"            AS medication_cost,
    "Follow-Up Visit Date"       AS followup_date,
    "Patient Satisfaction Score" AS satisfaction_score,
    "Referral Source"            AS referral_source,
    "Emergency Visit"            AS emergency_visit,
    "Payment Status"             AS payment_status,
    "Discharge Date"             AS discharge_date,
    "Admitted Date"              AS admitted_date,
    "Room Type"                  AS room_type,
    TRY_TO_NUMBER(REPLACE("Insurance Coverage", '%','')) AS insurance_coverage,
    "Room Charges(daily rate)"   AS room_daily_rate

FROM {{ source('raw', 'visits') }}

{% if is_incremental() %}
WHERE "Date of Visit" > (SELECT MAX(visit_date) FROM {{ this }})
{% endif %}