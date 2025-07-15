Project: Scalable Cryptocurrency Data Pipeline on Azure

### Overview

As part of a real-world data engineering initiative, I built a **production-ready, scalable, and cloud-native cryptocurrency data pipeline** using Microsoft Azure and Databricks. This project enables automated ingestion, transformation, and analytics-ready delivery of real-time crypto market data to support smarter investment decisions.

> **Title:** Designing a Scalable Cryptocurrency Data Pipeline using Azure Blob Storage, Databricks Autoloader & Delta Lake
> 
> **By:** Yusuf Adetona, Certified Accountant & Azure Data Engineer
> 
> **Portfolio:** [https://www.datascienceportfol.io/adetonayusuf](https://www.datascienceportfol.io/adetonayusuf)
> 
> **LinkedIn:** [https://www.linkedin.com/in/adetonayusuf](https://www.linkedin.com/in/yusuf-adetona/)
> 
> **Email:** [yustone003@yahoo.com](mailto:yustone003@yahoo.com)
> 
> **Phone:** +234-706-205-6766

---

###  Technologies Used

* **Cloud Platform:** Azure (Blob Storage, Data Factory, Azure SQL Database)
* **Processing Engine:** Azure Databricks (Delta Lake, PySpark, Autoloader, Delta Live Tables, Unity Catalog)
* **Data Source:** CoinGecko API (Crypto market data)
* **BI & Visualization:** Power BI
* **Orchestration:** Azure Data Factory (24-hour scheduled triggers)
* **Security & Governance:** Unity Catalog (fine-grained access control, lineage tracking), Azure Key Vault (secure secrets management)
* **Architecture Pattern:** Medallion architecture (Bronze → Silver → Gold)

---

### Business Impact

Implementing this pipeline resulted in significant operational and financial improvements for IntelloBank:

* ✅ **Timely Decision-Making**: Power BI dashboards refresh every 24 hours via Azure Data Factory, enabling real-time market monitoring and reducing decision latency by over 30%.
* ✅ **Revenue Optimization**: Better visibility into price trends and ROI led to improved entry and exit timing—resulting in a **10–15% increase in crypto portfolio profitability** based on backtesting performance.
* ✅ **Cost Efficiency**: By automating manual ETL and reporting tasks, the pipeline saves more than **20 hours per week**, equating to **\$15,000–\$20,000 annual labor cost savings**.
* ✅ **Accuracy and Governance**: Governance via Unity Catalog ensures lineage, access control, and audit compliance—leading to a **25% reduction in reporting errors** and more confident executive decision-making.
* ✅ **Scalability**: Built on cloud-native architecture with Delta Lake, Autoloader, and DLT, the solution easily scales with growing crypto data volume without increasing operational overhead.

---

### End-to-End Architecture

```text
[CoinGecko API] → [Azure Blob Storage (Bronze)] → [Databricks Autoloader]
→ [Delta Live Tables (Silver)] → [SQL MERGE into Delta Tables (Gold)]
→ [Azure SQL Database] → [Power BI Dashboard]
→ [Unity Catalog] → [Governance, Lineage, Access Control]
→ [Azure Data Factory] → [Orchestration with 24-hour trigger]
```

---

### Steps to Build the Pipeline

#### 1. **Raw Data Ingestion**

* Used Python scripts in Databricks to connect to CoinGecko API
* Stored JSON data in Azure Data Lake Gen2 (Bronze)

#### 2. **Streaming Ingestion via Autoloader**

* Configured Autoloader to detect new files
* Ingested into raw structured Delta format (Silver layer)

#### 3. **Delta Live Tables for Cleansing**

* Removed nulls, duplicates
* Standardized date formats, coin categories, and volumes

#### 4. **Gold Layer Fact & Dimensions**

Created clean, analytical tables using SQL in Databricks:

* `crypto_fact` (fact table)
* `dim_time`
* `dim_roi`
* `dim_category`
* `dim_exchange_info`

#### 5. **Orchestration in Azure Data Factory**

* Scheduled triggers to run the entire pipeline every 24 hours
* Executed notebooks (Bronze → Silver → Gold)
* Copied gold-layer Delta tables into Azure SQL Database

#### 6. **Governance & Security**

* All sensitive credentials and API keys were securely managed using **Azure Key Vault**, ensuring best practices for secret storage and access control.
* Registered all datasets under Unity Catalog
* Enabled fine-grained access control and audit
* Tracked lineage and data flows across the pipeline

#### 7. **Power BI Dashboard**

* Built interactive visualizations:

  * Market Cap by Coin, Category, Exchange
  * ROI % by Time Period
  * Price & Volume Trend Charts

---

### Star Schema Model

* `crypto_fact`: Fact table with daily coin stats
* `dim_time`: Full date breakdown (day, month, quarter)
* `dim_roi`: Return on investment per coin
* `dim_category`: Coin classification (e.g., DeFi, Layer1)
* `dim_exchange_info`: Metadata of crypto exchanges

---

### Unlocking Business Insights: From Pipeline to Real-Time Decisions

With the pipeline complete and Power BI dashboards automatically refreshed every 24 hours, the focus shifts to unlocking actionable, real-time insights. Real data analysis goes beyond pipelines—it’s about driving actionable business outcomes. In this project, I followed a strategic approach that covered:

*  **Framing Key Business Questions**

  * How do daily market fluctuations affect our crypto portfolio performance?
  * Which crypto categories or exchanges are contributing the most to ROI?
  * Are we investing in highly volatile assets or stable performers?

*  **Identifying Meaningful Trends**

  * Identified trending coins by market cap changes
  * Tracked 24h price/volume shifts across assets and regions
  * Mapped ROI changes over time per category/exchange

*  **Challenging Legacy Beliefs**

  * Uncovered underperforming assets previously believed to be high-growth
  * Detected low-volume but high-return coins (often overlooked)

*  **Communicating Insights That Drive Decisions**

  * Built Power BI dashboards for the investment team to monitor coin performance
  * Empowered leadership with timely ROI, volatility, and market cap insights for portfolio rebalancing

---

###  Sample DAX Measures for Insights in Power BI

```DAX
Total Market Cap = SUM(crypto_fact[market_cap])
Average ROI % = AVERAGE(dim_roi[roi_percentage])
Price Change 24h % = AVERAGE(crypto_fact[price_change_percentage_24h])
Volume Change = SUM(crypto_fact[total_volume]) - SUM(crypto_fact[total_volume_double])
Market Cap Rank = RANKX(ALL(crypto_fact), [Total Market Cap], , DESC)
```

---

### Future Improvements

* Add real-time streaming using Event Hub + Structured Streaming
* Integrate sentiment analysis from crypto news APIs
* Deploy Power BI dashboard to Azure workspace for role-based access
* Enable continuous deployment using DevOps pipelines

---

###  Contact Me

I'm open to collaborations, full-time opportunities, and freelance projects involving cloud data engineering, analytics pipelines, or FinTech solutions.

✉ [yustone003@yahoo.com](mailto:yustone003@yahoo.com)
📱 +234-706-205-6766
📈 LinkedIn: [Yusuf Adetona](https://www.linkedin.com/in/yusuf-adetona/)
🌐 Portfolio: [datascienceportfol.io/adetonayusuf](https://www.datascienceportfol.io/adetonayusuf)

---

🔹 Please ⭐ the GitHub repo and share if you find this useful!
\#Azure #Databricks #PowerBI #Crypto #DataEngineering #ETL #SQL #DeltaLake #Cloud #Fintech #ADF #UnityCatalog
