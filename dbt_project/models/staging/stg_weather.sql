-- Staging: one row per hour of weather, same columns as silver.

select
    observed_at,
    temp_c,
    precipitation_mm,
    wind_kmh,
    weather_code,
    year_month
from {{ source('silver', 'weather') }}
