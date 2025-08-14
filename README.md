# Air Quality & Weather Data Wrangling – Rome 2024

## Overview
This project is part of the **Udacity Data Analysis Nanodegree**.  
It focuses on gathering, cleaning, and analyzing two related datasets:  
- **Weather conditions** in Rome for the year 2024  
- **Air quality measurements** (PM2.5 and NO₂) in Rome for the same period  

The aim is to demonstrate skills in **data wrangling**, **API usage**, and **exploratory data analysis**.

---

## Datasets

### Dataset 1 – Weather Data
- **Source:** [Open-Meteo Archive API (ERA5)](https://open-meteo.com/)
- **Method:** Programmatic retrieval via API
- **Variables:**
  - `date` — date of observation (daily)
  - `temp_c` — average daily temperature in Celsius
  - `rhum_pct` — average daily relative humidity (%)
  - `wind_speed_ms` — average daily wind speed (m/s)
  - `precip_mm` — daily total precipitation (mm)

### Dataset 2 – Air Quality Data
- **Source:** [European Environment Agency (EEA)](https://www.eea.europa.eu/)
- **Method:** CSV file
- **Variables:**
  - `date` — date of observation (daily)
  - `pm25` — daily average PM2.5 concentration (µg/m³)
  - `no2` — daily average NO₂ concentration (µg/m³)

---

## Project Steps
1. **Gather** – Retrieve data from APIs using Python requests and custom loader modules.
2. **Assess** – Identify data quality and tidiness issues (e.g., missing values, inconsistent formats).
3. **Clean** – Fix data types, handle missing values, ensure datasets can be merged.
4. **Merge** – Combine datasets on the `date` column.
5. **Analyze** – Explore the relationship between weather conditions and air pollution in Rome.
6. **Visualize** – Create plots to illustrate trends and correlations.

---

## Research Question
> How do weather conditions influence air quality levels (PM2.5 and NO₂) in Rome during 2024?

---

## Requirements
Install the necessary Python libraries:
```bash
pip install -r requirements.txt
