# Mercury-Invest: End-to-End Stock Analysis & Portfolio Optimization

**Mercury-Invest** is a financial analytics platform that automates **US stock market** data ingestion, forecasting, and portfolio optimization. Using **Apache Spark**, **Delta Lake**, **Airflow**, **Docker**, and **ML frameworks** like Scikit-learn or TensorFlow within a **Medallion Architecture** (Bronze → Silver → Gold), it showcases production-level data engineering combined with quantitative finance capabilities.

---

## Key Features

- **Real-World Architecture**: Automates data ingestion, transformations, and analytics using Apache Airflow, Spark, and Delta Lake.  
- **Integrated Analytics**: Combines **machine learning** models for stock forecasting and **Modern Portfolio Theory** for optimized allocations.  
- **Scalable & Reproducible**: Docker-based environment ensures seamless scaling, deployment, and maintenance.  
- **Flexible Extensibility**: Supports additional data sources, advanced ML models, and new analytics requirements.

---

## Project Workflow Overview

**Mercury-Invest** automates data pipelines, prediction modeling, and portfolio management following these steps:

### 1. Automated Data Pipelines
- **Ingestion**: Airflow schedules daily/weekly data pulls from:
  - `yfinance` for stock market data.
  - **FRED** for macroeconomic indicators.
- Data is stored in the **Medallion Architecture**:
  - **Bronze**: Raw data.
  - **Silver**: Cleaned & joined data.
  - **Gold**: Analytics-ready datasets.

### 2. Scalable Data Transformations
- **Spark** performs large-scale ETL and feature engineering.
- **Delta Lake** ensures ACID compliance and versioning for tracking changes.

### 3. Machine Learning for Stock Forecasting
- **Models**: Predict stock returns or outperformance using:
  - **Scikit-learn** for traditional ML models (e.g., Random Forest).  
  - **TensorFlow** for time-series models like LSTM.  
- Ensures time-series validation to avoid lookahead bias.

### 4. Portfolio Optimization & Rebalancing
- Implements **Mean-Variance Optimization (MVO)**:
  - Assigns weights to maximize returns while minimizing portfolio risk.
  - Tracks metrics: Sharpe Ratio, drawdown, and volatility.
- Rebalancing frequencies: Monthly/quarterly.

### 5. Deployment Environment
- **Docker Containerization**:
  - Includes pipelines, transformations, and ML models for reproducible testing and production.

---

## Technology Stack

| Component               | Purpose                                                                 |
|-------------------------|-------------------------------------------------------------------------|
| **Apache Airflow**       | Schedules automated data workflows for ingestion and transformation.   |
| **Apache Spark**         | Handles massive data transformations and feature extraction.           |
| **Delta Lake**           | Provides transactional consistency (Bronze → Silver → Gold layers).    |
| **Scikit-learn**         | Builds predictive ML models for forecasting.                          |
| **TensorFlow**           | Supports advanced time-series forecasting (e.g., LSTM).               |
| **Docker**               | Ensures consistent, reproducible environments.                        |
| **Databricks (Optional)**| Scalable Spark environment for distributed data processing.            |
| **Power BI (Planned)**   | Creates real-time dashboards for performance monitoring.               |

---

## Usage Instructions

To replicate or extend the **Mercury-Invest** workflow:

1. **Automated Data Ingestion**
   - Use Airflow DAGs for scheduling weekly/daily data ingestion.
   - Pull data from:
     - `yfinance` (stocks).
     - **FRED** (macro).

2. **Data Transformation**
   - Clean and enrich data using Apache Spark.
   - Utilize Delta Lake to track historical versions (time-travel).

3. **Machine Learning**
   - Train ML models for forecasting. Available options include:
     - Scikit-learn-based predictive models.
     - TensorFlow-based models for time-series forecasting.

4. **Portfolio Optimization**
   - Run Mean-Variance Optimization and record rebalancing metrics.

5. **Containerized Execution**
   - Build and deploy the entire pipeline using **Docker** for consistency.

---

## Future Development Opportunities

| Expansion Area              | Description                                                          |
|-----------------------------|----------------------------------------------------------------------|
| **CI/CD Integration**        | Automate testing, linting, and container builds via GitHub Actions. |
| **Sector Data Inclusion**    | Leverage sector classification for greater analysis depth.           |
| **Advanced ML Models**       | Explore LSTM/Transformers for time-series predictions.              |
| **BI Dashboards**            | Enable real-time insights through Power BI dashboards.              |
| **Microsoft Fabric Migration** | Unified analytics platform with improved integration capabilities. |

---

## Architecture Flow (Bronze → Silver → Gold)

Add a **diagram** like this to visually illustrate your architecture (can be created using tools like Lucidchart or PowerPoint):

```txt
Ingestion (Airflow) --> Bronze --> Cleaning (Spark) --> Silver --> ML Models / Optimization --> Gold
```

---

## Contact & Disclaimer

This project **does not** constitute financial advice. It is intended as a learning-oriented implementation of end-to-end data analytics and financial optimization pipelines. For inquiries or contributions, please create a GitHub Issue.
