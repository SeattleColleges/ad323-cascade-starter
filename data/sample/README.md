# Frozen sample data

Committed on purpose. Graded work runs on these files so results are
reproducible and no assignment depends on a live download. **Do not edit or
regenerate them during the quarter**, because the reference answers depend on
them.

| Folder | Trips file | Rows | Weather file | Hours |
|---|---|---:|---|---:|
| `2026-09/` | `202609-divvy-tripdata.csv` | 50,000 (of 770,562) | `weather_chicago_hourly.json` | 720 |
| `2026-10/` | `202610-divvy-tripdata.csv` | 50,000 (of 768,705) | `weather_chicago_hourly.json` | 744 |

- **Trips** are a random sample of the full Divvy monthly file. They were
  built by `src/cascade/sample.py` with seed 323. Lines are copied byte for
  byte and kept in their original order.
- **Weather** is the full month of hourly Chicago weather from the Open-Meteo
  historical API (latitude 41.88, longitude -87.63, time zone
  America/Chicago).

The layout matches `data/bronze/<batch>/`, so ingestion can copy from here:
`uv run python -m cascade.ingest --batch 2026-09 --source data/sample`.

## Licenses

- Trip data: [Divvy Data License Agreement](https://divvybikes.com/data-license-agreement)
  (Lyft Bikes and Scooters, LLC). Copying and redistribution are allowed;
  selling the data as a standalone dataset is not.
- Weather data by [Open-Meteo.com](https://open-meteo.com/), licensed
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
