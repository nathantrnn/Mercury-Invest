# MercuryInvest: End-to-End Stock Analysis & Portfolio Optimization

**MercuryInvest** is a tangible data engineering and data science project focused on the **US stock market**. It employs **Apache Spark**, **Delta Lake**, **Airflow**, **Docker**, and **Python ML libraries** (Scikit-learn & TensorFlow) within a **Medallion Architecture** (Bronze → Silver → Gold). By automating **stock and macroeconomic data ingestion**, running **machine learning** models for returns forecasting, and applying **Modern Portfolio Theory** for periodic rebalancing, MercuryInvest illustrates a production-level approach to financial analytics.

---

## Project Overview

1. **Automated Data Pipelines**  
   - **Apache Airflow** orchestrates daily/weekly data ingestion from `yfinance` (stocks) and **FRED** (macroeconomic indicators), storing raw files in a **Delta Lake**.
   - Emphasizes a **Medallion Architecture**—raw data (Bronze), refined data (Silver), and final analytics (Gold).

2. **Scalable Data Transformations**  
   - **Apache Spark** handles large-scale ETL, computing technical indicators and joining stock returns with macro factors.
   - Maintains robust transaction integrity through **Delta Lake** features like time-travel and ACID compliance.

3. **Machine Learning for Stock Forecasting**  
   - Predicts future returns or probabilities of outperformance with **Scikit-learn** or **TensorFlow** models.
   - Incorporates time-series splitting to avoid lookahead bias, increasing reliability of results.

4. **Portfolio Construction & Rebalancing**  
   - Applies **Mean-Variance Optimization** (Modern Portfolio Theory) to allocate weights based on forecasted risk and returns.
   - Rebalancing occurs monthly or quarterly, with metrics like Sharpe Ratio and drawdown stored for historical tracking.

5. **Containerized Environment**  
   - **Docker** encapsulates services (Airflow, Spark, etc.) for a reproducible local or cloud-ready environment.
   - **Databricks Community Edition** (optional) extends Spark capabilities in a free, hosted environment.

---

## Why MercuryInvest is Relevant

- **Real-World Architecture**  
  Implements industry-standard pipeline practices: **Airflow** scheduling, **Spark** ETL at scale, and **Delta Lake** for ACID reliability.

- **Integrated Financial Analytics**  
  Combines quantitative finance concepts (portfolio optimization) with modern machine learning workflows.  
  Creates advanced analytics aligned with production-level investment pipelines.

- **Operational & Reproducible**  
  **Docker** ensures portability across development, testing, and deployment environments.  
  Potential employers can see practical code that’s designed for scaling, debugging, and monitoring.

- **Flexible & Extensible**  
  Adding new data sources, advanced models (like deep learning), or extra risk constraints is straightforward.  
  Illustrates a design that can adapt as business or market requirements evolve.

---

## Technology Stack

- **Airflow**: Orchestrates ingestion, transformations, and rebalancing tasks on a set schedule.  
- **Spark & Delta Lake**: Large-scale data processing, ACID transactions, and time-travel queries.  
- **Docker**: Containerizes the pipeline components for consistency and easy deployment.  
- **Scikit-learn / TensorFlow**: Implements machine learning pipelines from feature engineering to inference.  
- **Local/Hybrid Data Lake**: Structuring the pipeline in Bronze, Silver, Gold layers for clarity and maintainability.  
- **Databricks Community Edition**: Optional environment for additional Spark-based experimentation.

---

## Potential Expansion

- **CI/CD Integration**: Streamline testing and deployment using GitHub Actions or other pipelines.  
- **Additional Data Sources**: Incorporate fundamentals, sentiment data, or alternative asset classes.  
- **Enhanced Modeling**: Explore LSTM or Transformers for time-series forecasting.  
- **Comprehensive Dashboards**: Present real-time portfolio performance in BI tools like Power BI or Streamlit.

---

## Contact & Disclaimer

This project **does not** constitute financial advice; it’s intended as a real-world data pipeline and analytics implementation for educational and professional development. For any inquiries, suggestions, or collaborative ideas, please reach out via **GitHub Issues** or your preferred communication channel.
