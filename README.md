# Medallion Data Pipeline

A comprehensive **ETL pipeline** implementing the **Medallion Architecture** (Bronze, Silver, Gold layers).  
The pipeline processes mobility_dataset from **Google Sheets** into **PostgreSQL**, with robust data validation, quality checks, and audit logging.

---

## 🏛️ What is the Medallion Architecture?

The **Medallion Architecture** is a **data design pattern** for building scalable, reliable, and high-quality data pipelines.  
It organizes data into **progressive layers** — Bronze, Silver, and Gold — ensuring that each step improves **data quality** and **business value**.

- **Bronze Layer (Raw):** Stores raw ingested data with minimal transformations.  
- **Silver Layer (Clean):** Cleansed, validated, and standardized data with enforced quality checks.  
- **Gold Layer (Analytics):** Curated, aggregated datasets ready for reporting, dashboards, and business KPIs.  

---

## 🏗️ Architecture Overview

- 🥉 **Bronze Layer**  
  Raw data ingestion from **Google Sheets**  
  ✅ COMPLETE  

- 🥈 **Silver Layer**  
  Cleaned, validated, and transformed data  
  ✅ COMPLETE  

- 🥇 **Gold Layer**  
  Business analytics and KPIs  
  🚧 READY FOR DEVELOPMENT  

---

## 🔄 Dataflow

```mermaid
flowchart TD
    GS[Google Sheets] --> B[🥉 Bronze Layer: Raw Storage]
    B --> S[🥈 Silver Layer: Cleaned & Validated]
    S --> G[🥇 Gold Layer: Analytics & KPIs]

    B --> RS[Raw Storage]
    S --> QC[Data Quality Checks]
    S --> AL[Audit Logging]
    S --> VR[Validation Rules]
