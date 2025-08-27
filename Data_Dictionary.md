# 📘 Data Dictionary – Medallion Data Pipeline

This document defines the schemas across the **Medallion Architecture**:  
🥉 Bronze (Raw) → 🥈 Silver (Cleaned & Validated) → 🥇 Gold (Analytics & BI).  

---

## 🥉 Bronze Layer (Raw Data)

The **Bronze Layer** stores raw ingested data from Google Sheets with minimal transformations.  
It preserves the source-of-truth input as-is for reproducibility.

### Table: `bronze.drivers`
| Column         | Type   | Description |
|----------------|--------|-------------|
| driver_id      | TEXT (PK) | Unique driver identifier |
| name           | TEXT   | Full name of driver |
| phone          | TEXT   | Contact number |
| email          | TEXT   | Email address |
| signup_date    | DATE   | Registration date |

---

### Table: `bronze.vehicles`
| Column         | Type   | Description |
|----------------|--------|-------------|
| vehicle_id     | TEXT (PK) | Vehicle identifier |
| driver_id      | TEXT (FK) | Driver associated with vehicle |
| make           | TEXT   | Vehicle manufacturer |
| model          | TEXT   | Vehicle model |
| year           | INT    | Year of manufacture |
| color          | TEXT   | Vehicle color |
| capacity       | INT    | Seating capacity |

---

### Table: `bronze.riders`
| Column         | Type   | Description |
|----------------|--------|-------------|
| rider_id       | TEXT (PK) | Unique rider identifier |
| name           | TEXT   | Rider name |
| phone          | TEXT   | Contact number |
| email          | TEXT   | Email address |
| signup_date    | DATE   | Rider registration date |

---

### Table: `bronze.trips`
| Column         | Type   | Description |
|----------------|--------|-------------|
| trip_id        | TEXT (PK) | Trip identifier |
| driver_id      | TEXT (FK) | Driver for trip |
| vehicle_id     | TEXT (FK) | Vehicle used |
| rider_id       | TEXT (FK) | Rider taking the trip |
| request_ts     | TIMESTAMP | Request timestamp |
| pickup_ts      | TIMESTAMP | Pickup timestamp |
| dropoff_ts     | TIMESTAMP | Drop-off timestamp |
| pickup_location| TEXT   | Pickup city/location |
| drop_location  | TEXT   | Drop-off city/location |
| distance_km    | NUMERIC | Trip distance in km |
| duration_min   | NUMERIC | Trip duration in minutes |
| surge_multiplier | NUMERIC | Surge factor applied |

---

### Table: `bronze.payments`
| Column         | Type   | Description |
|----------------|--------|-------------|
| payment_id     | TEXT (PK) | Payment identifier |
| trip_id        | TEXT (FK) | Related trip |
| amount_usd     | NUMERIC | Payment amount in USD |
| tip_usd        | NUMERIC | Tip amount |
| tax_usd        | NUMERIC | Tax applied |
| payment_method | TEXT   | Card / Cash / Wallet |
| status         | TEXT   | Payment status (SUCCESS/FAILED/etc.) |

---

## 🥈 Silver Layer (Cleaned & Validated)

The **Silver Layer** applies cleaning, standardization, and DQ validation rules.  
This layer ensures referential integrity and prepares structured data for analytics.

### Table: `silver.drivers`
| Column      | Type   | Description |
|-------------|--------|-------------|
| driver_id   | TEXT (PK) | Cleaned driver ID |
| name        | TEXT   | Driver name |
| email       | TEXT   | Validated email |
| phone       | TEXT   | Standardized phone |
| city        | TEXT   | City of registration |
| signup_date | DATE   | Registration date |

---

### Table: `silver.vehicles`
| Column      | Type   | Description |
|-------------|--------|-------------|
| vehicle_id  | TEXT (PK) | Vehicle ID |
| driver_id   | TEXT (FK) | Linked driver |
| make        | TEXT   | Vehicle make |
| model       | TEXT   | Vehicle model |
| year        | INT    | Manufacturing year |
| capacity    | INT    | Seats available |
| color       | TEXT   | Color |

---

### Table: `silver.riders`
| Column      | Type   | Description |
|-------------|--------|-------------|
| rider_id    | TEXT (PK) | Rider ID |
| name        | TEXT   | Rider name |
| email       | TEXT   | Valid email |
| phone       | TEXT   | Standardized phone |
| signup_date | DATE   | Rider signup date |

---

### Table: `silver.trips`
| Column      | Type   | Description |
|-------------|--------|-------------|
| trip_id     | TEXT (PK) | Trip ID |
| driver_id   | TEXT (FK) | Driver |
| vehicle_id  | TEXT (FK) | Vehicle |
| rider_id    | TEXT (FK) | Rider |
| request_ts  | TIMESTAMP | Request timestamp |
| pickup_ts   | TIMESTAMP | Pickup time |
| dropoff_ts  | TIMESTAMP | Dropoff time |
| pickup_location | TEXT | Pickup city |
| drop_location | TEXT   | Drop-off city |
| distance_km | NUMERIC | Cleaned trip distance |
| duration_min| NUMERIC | Cleaned trip duration |
| wait_time_minutes | NUMERIC | Wait time before pickup |
| surge_multiplier | NUMERIC | Surge applied |

---

### Table: `silver.payments`
| Column      | Type   | Description |
|-------------|--------|-------------|
| payment_id  | TEXT (PK) | Payment ID |
| trip_id     | TEXT (FK) | Related trip |
| fare_usd    | NUMERIC | Base fare amount |
| tip_usd     | NUMERIC | Tip |
| tax_usd     | NUMERIC | Tax |
| total_usd   | NUMERIC | Fare + Tip + Tax |
| method      | TEXT   | Payment method |
| status      | TEXT   | Payment status |

---

## 🥇 Gold Layer (BI & Analytics)

The **Gold Layer** transforms Silver data into BI-ready aggregates, KPIs, and a dashboard fact table.  
Includes reconciliation checks against Silver.

### Table: `gold.driver_stats`
| Column            | Type    | Description |
|-------------------|---------|-------------|
| driver_id         | TEXT (FK) | References `silver.drivers.driver_id` |
| total_trips       | INT     | Number of trips completed by the driver |
| total_earnings_usd| NUMERIC | Total earnings (sum of payments) |
| avg_trip_fare_usd | NUMERIC | Average trip fare |
| avg_tip_usd       | NUMERIC | Average tip amount |
| avg_tip_rate      | NUMERIC | Average tip % (tip ÷ fare) |
| tip_take_rate     | NUMERIC | Share of trips with tip > 0 |
| global_avg_tip_usd| NUMERIC | Platform-wide average tip |

---

### Table: `gold.vehicle_stats`
| Column          | Type    | Description |
|-----------------|---------|-------------|
| vehicle_id      | TEXT (PK) | Vehicle ID |
| driver_id       | TEXT (FK) | Driver |
| total_trips     | INT     | Trips by vehicle |
| total_revenue_usd | NUMERIC | Total revenue |
| avg_duration_min | NUMERIC | Avg trip duration |
| avg_distance_km | NUMERIC | Avg trip distance |

---

### Table: `gold.rider_stats`
| Column          | Type    | Description |
|-----------------|---------|-------------|
| rider_id        | TEXT (FK) | Rider |
| total_trips     | INT     | Trips by rider |
| total_spend_usd | NUMERIC | Total spend |
| avg_trip_fare_usd | NUMERIC | Avg fare |
| first_trip_date | DATE    | First trip |
| last_trip_date  | DATE    | Latest trip |

---

### Table: `gold.daily_kpis`
| Column                  | Type    | Description |
|--------------------------|---------|-------------|
| trip_date               | DATE    | Trip date |
| trips                   | INT     | Daily trips |
| active_drivers          | INT     | Unique drivers active |
| active_riders           | INT     | Unique riders active |
| total_revenue_usd       | NUMERIC | Total revenue |
| avg_revenue_per_trip_usd| NUMERIC | Avg revenue per trip |

---

### Table: `gold.city_kpis`
| Column         | Type    | Description |
|----------------|---------|-------------|
| city           | TEXT    | City |
| total_pickups  | INT     | Pickups in city |
| total_dropoffs | INT     | Drop-offs in city |
| avg_fare_usd   | NUMERIC | Avg fare |
| avg_revenue_usd| NUMERIC | Avg revenue |
| total_revenue_usd | NUMERIC | Total revenue |
| unique_drivers | INT     | Unique drivers in city |
| unique_riders  | INT     | Unique riders in city |

---

### Table: `gold.dashboard`
Flattened trip-level fact table for BI.

| Column            | Type    | Description |
|-------------------|---------|-------------|
| trip_id           | TEXT (PK) | Trip identifier |
| trip_date         | DATE    | Derived trip date |
| request_ts        | TIMESTAMP | Request time |
| pickup_ts         | TIMESTAMP | Pickup time |
| dropoff_ts        | TIMESTAMP | Dropoff time |
| driver_id         | TEXT (FK) | Driver |
| vehicle_id        | TEXT (FK) | Vehicle |
| rider_id          | TEXT (FK) | Rider |
| pickup_location   | TEXT    | Pickup city |
| drop_location     | TEXT    | Drop-off city |
| distance_km       | NUMERIC | Distance |
| duration_min      | NUMERIC | Duration |
| wait_time_minutes | NUMERIC | Wait time |
| surge_multiplier  | NUMERIC | Surge factor |
| base_fare_usd     | NUMERIC | Base fare |
| tax_usd           | NUMERIC | Tax |
| tip_usd           | NUMERIC | Tip amount |
| total_fare_usd    | NUMERIC | Total fare |
| payment_method    | TEXT    | Payment method |
| fare_usd          | NUMERIC | Fare amount (from payments) |
| tip_usd_payment   | NUMERIC | Tip (from payments) |
| payment_status    | TEXT    | Payment status |
| make              | TEXT    | Vehicle make |
| model             | TEXT    | Vehicle model |
| year              | INT     | Vehicle year |
| capacity          | INT     | Seating |
| color             | TEXT    | Color |
| driver_city       | TEXT    | Driver’s city |

---

## 🗂 Audit & Reconciliation
The Gold layer logs reconciliation in **`audit.recon_results`**:

| Column           | Type    | Description |
|------------------|---------|-------------|
| run_id           | TEXT    | Pipeline run ID |
| check_name       | TEXT    | Check performed |
| lhs_value        | NUMERIC | Silver reference value |
| rhs_value        | NUMERIC | Gold computed value |
| diff             | NUMERIC | Difference |
| within_tolerance | BOOLEAN | Pass/fail tolerance |
| created_at       | TIMESTAMP | Execution time |

Checks include:
- Trip counts (Silver vs Gold Dashboard)  
- Tip sums (Silver vs Gold Dashboard)  
- Driver counts (Silver vs Driver Stats)  
- Rider counts (Silver vs Rider Stats)  

---
