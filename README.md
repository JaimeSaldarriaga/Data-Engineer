# Data-Engineer
Data-Engineer
Data-Engineer
Prueba Tecnica Project : Dual Pipeline for Real-Time & Batch Market Data Processing Objective:

Build a data pipeline that features two parallel workflows—one for real-time data ingestion and one for batch processing—both following a Bronze–Silver–Gold medallion architecture and feeding into a unified data quality monitoring dashboard. For this task you will use the btc csv that is uploaded to this repo. Key Components:

Data Sources: Real-Time Data Source: A simulated or live feed from the Binance API or other for BTC/USD market data, which delivers real-time price, volume, and order book information. Make sure to search for other Data sources(hint: Open source APIs). The real time data ingestion can be simulated by making multiple API calls every couple of minutes, in case a live API source is not available. You will find a news CSV in this repo that you can use for analysis, this reflects the type of data we will be handling day to day. Make sure to use this csv in your report and as a part of the pipeline. 

Batch Data Source: A historical dataset in CSV format from CryptoCompare (or a similar provider) that contains past BTC/USD market data with minute-level granularity. In the repo you can find a dataset of bitcoin news.
Pipelines
Real-Time Pipeline: Bronze: Simulate periodic API calls (e.g., every 30 seconds) to ingest raw BTC/USD market data.
Silver: Clean and standardize the data (e.g., format timestamps, handle missing values).
Gold: Aggregate key metrics (e.g., moving averages, volume summaries) and perform automated data quality checks.
Output: Feed immediate alerts and metrics into a live dashboard.
Batch Pipeline:
Bronze: Ingest historical or scheduled market data snapshots (e.g., hourly or daily).
Silver: Process the batch data similarly—clean and transform it into a structured format.
Gold: Generate comprehensive summaries and trend analyses with integrated data quality tests.
Output: Aggregate insights are displayed alongside real-time metrics on the dashboard.
Unified Dashboard: Build a lightweight dashboard (using tools like Streamlit, Power BI, etc.) that displays both real-time quality alerts and batch-aggregated insights, offering a complete picture of your data’s health and trends.
Technology Stack Options:
Python-Only: Use packages like Pandas, Requests, and APScheduler to implement the entire pipeline in code, ensuring simplicity and full Git version control.
Transformation Frameworks: Leverage dbt for data transformations and quality testing.
Orchestration Tools/Cloud Platforms: Consider using Azure Data Factory, Apache Airflow, Prefect, or similar tools for scheduling and orchestration. 
Project Deliverables:

Codebase & Environment:

A fully version-controlled repository (e.g., GitHub with Codespaces integration) containing all pipeline scripts, configurations, and documentation.

Data Visualizations:

Interactive visualizations such as charts or graphs that display live market metrics (e.g., price trends, moving averages, trading volumes) and highlight any data quality issues.

Monitoring Dashboard:

A unified dashboard (using tools like Streamlit, Power BI, or a web interface) that provides real-time updates from the live pipeline alongside aggregated batch summaries. The dashboard should offer high-level insights into data health and market trends.

This project is designed to be completed in 2–4 hours and allows you to showcase your expertise in building scalable, maintainable data pipelines with both real-time and batch processing capabilities—all while keeping the entire solution in a Git-controlled codebase. The focus of this project is more about the way you get to the solution, than the final solution, we measure knowledge, not codekeeping the entire solution in a Git-controlled codebase. The focus of this project is more about the way you get to the solution, than the final solution, we measure knowledge, not code.
Please generate a REPO we can run to see the results you get. 
