# Medallion Data Pipeline

A comprehensive **ETL pipeline** implementing the **Medallion Architecture** (Bronze, Silver, Gold layers) that processes **supply chain data** from Google Sheets through PostgreSQL with complete data validation, quality checks, and audit logging.

---

## 🏗️ Architecture Overview

### Medallion Layers

- 🥉 **Bronze Layer**  
  Raw data ingestion from Google Sheets  
  ✅ COMPLETE  

- 🥈 **Silver Layer**  
  Cleaned, validated, and transformed data  
  ✅ COMPLETE  

- 🥇 **Gold Layer**  
  Business analytics and KPIs  
  🚧 READY FOR DEVELOPMENT  

---

## 📖 Table of Contents
1. [Introduction](#introduction)  
2. [Installation](#installation)  
3. [Usage](#usage)  
4. [Features](#features)  
5. [Dependencies](#dependencies)  
6. [Configuration](#configuration)  
7. [Examples](#examples)  
8. [Troubleshooting](#troubleshooting)  
9. [Contributors](#contributors)  
10. [License](#license)  

---

## 📌 Introduction
This project demonstrates a **modern data engineering pipeline** following the **Medallion Architecture** to ensure scalability, reliability, and high data quality. The pipeline ingests supply chain datasets, validates them, and prepares them for analytics dashboards.

---

## ⚙️ Installation
```bash
# Clone the repository
git clone https://github.com/ANURAGBALLER6/Medallion-Data-Pipeline.git
cd Medallion-Data-Pipeline

# (Optional) Create and activate virtual environment
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows

# Install dependencies
pip install -r requirements.txt
