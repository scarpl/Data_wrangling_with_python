# Data Wrangling with Python – Rome Weather & Air Quality

This repository contains the final project for the **Data Wrangling with Python** course.  
The goal is to gather, assess, clean, and analyze two real-world datasets in order to answer a research question about the relationship between **weather conditions** and **air quality** in Rome.

---

## 📂 Project Structure

```
├── data/
│   ├── raw/                # Raw datasets before cleaning
│   │   ├── air_quality_rome_2022.csv
│   │   ├── air_quality_rome_2023.csv
│   │   └── weather_rome_2022_2024.csv
│   ├── cleaned/            # Cleaned datasets after wrangling
│   └── merged/             # Final dataset (merged and tidy)
├── modules/                # Custom Python modules for data gathering
│   ├── openaq_loader.py    # Fetch and save daily air quality data from OpenAQ API
│   └── openmeteo_weather.py # Fetch and process daily weather data from Open-Meteo API
├── data_wrangling_project_filled.ipynb # Main notebook with all steps
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

---

## 📊 Datasets

### Dataset 1 – Weather Data
- **Source:** [Open-Meteo API](https://open-meteo.com/)  
- **Method:** Programmatic API request using a custom Python module (`openmeteo_weather.py`).  
- **Variables:**
  - `date` — date of observation  
  - `temp_c` — daily average temperature (°C)  
  - `rhum_pct` — average relative humidity (%)  
  - `wind_speed_ms` — average wind speed (m/s)  
  - `precip_mm` — daily precipitation (mm)  

### Dataset 2 – Air Quality Data
- **Source:** [OpenAQ API](https://openaq.org/)  
- **Method:** Programmatic API request, aggregated daily, and saved as CSV (`openaq_loader.py`).  
- **Variables:**
  - `date` — date of observation  
  - `city` — observation city (Rome)  
  - `pm25` — daily average PM2.5 concentration (µg/m³)  
  - `no2` — daily average NO₂ concentration (µg/m³)  

---

## ⚙️ Project Steps

1. **Gathering**  
   - Weather data via **Open-Meteo API** (JSON → DataFrame).  
   - Air quality data via **OpenAQ API**, saved as CSV locally.  

2. **Assessing**  
   - Checked for missing values, duplicates, and outliers.  
   - Identified data quality issues (e.g., NaN values, extreme outliers in precipitation).  
   - Identified tidiness issues (pollutants stored as separate columns instead of a variable).  

3. **Cleaning**  
   - Converted date columns to proper datetime format.  
   - Removed duplicates and handled missing values.  
   - Reshaped the air quality dataset into tidy format with `pd.melt()`.  

4. **Storing**  
   - Saved cleaned datasets to `/data/cleaned/`.  
   - Produced a merged tidy dataset in `/data/merged/`.  

5. **Analysis & Visualization**  
   - Explored relationships between weather variables and pollutant concentrations.  
   - Created visualizations (boxplots, scatterplots, line charts).  
   - Justified outliers (e.g., heavy rainfall days are meteorologically plausible).  

---

## ❓ Research Question

**How do weather conditions (temperature, humidity, wind speed, precipitation) affect air quality (PM2.5 and NO₂) in Rome between 2022 and 2024?**

---

## 📈 Results

- Higher wind speeds are generally associated with **lower pollutant concentrations**, as wind disperses particles.  
- Heavy rainfall events correspond to **outliers** in precipitation but are meteorologically valid and often associated with cleaner air afterwards.  
- NO₂ concentrations are higher in colder months, consistent with traffic and heating emissions.  

---

## 🚀 Next Steps

If given more time:  
- Extend analysis to multiple cities (e.g., Milan, Naples) to compare patterns.  
- Integrate additional pollutants (O₃, CO).  
- Apply regression or machine learning models to quantify relationships.  

---

## 🛠 Requirements

Install dependencies with:

```bash
pip install -r requirements.txt
```

Main packages:
- `pandas`
- `numpy`
- `matplotlib`
- `requests`

---

## 👤 Author

**Luca Scarpantonio**  
Data Wrangling with Python – Final Project  
