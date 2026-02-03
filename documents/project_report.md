# Project Report
## End-to-End Sales & Marketing Analytics  
### Maven Fuzzy Factory

---

## 1. Project Overview

This project delivers an end-to-end analytics solution for **Maven Fuzzy Factory**, an e-commerce retailer selling teddy bear products online.  
The goal was to transform raw transactional and marketing data into **actionable business insights** using a modern analytics stack.

The solution covers the full lifecycle:
- Raw data understanding
- Data modeling and transformation
- Analytical layer creation
- Interactive dashboards for business users

---

## 2. Business Problem

Maven Fuzzy Factory collects large volumes of data related to:
- Website traffic
- Marketing campaigns
- Orders and order items
- Product refunds

However, this data exists in raw, normalized tables that are difficult for business stakeholders to analyze directly.

The company needs answers to questions such as:
- Which marketing channels and campaigns drive revenue?
- How efficiently does traffic convert into customers?
- Which products are profitable and which have high refund rates?
- How do performance trends change over time and by device?

Without a structured analytics layer, decision-making relies on ad-hoc queries and manual analysis.

---

## 3. Data Sources

The project uses the following raw tables:

- `orders`
- `order_items`
- `order_item_refunds`
- `products`
- `website_sessions`
- `website_pageviews`

Key characteristics:
- Sales data is captured at the **order-item level**
- Marketing data is captured at the **session level**
- Refunds occur at the **order-item level**

These characteristics informed all downstream modeling decisions.

---

## 4. Data Modeling & Architecture

### 4.1 Architecture Overview

The project follows a **modern analytics architecture**:

Raw CSV Data
↓
Python (Data Cleaning & Validation)
↓
Databricks (Delta Gold Layer)
↓
Power BI (Semantic Model & Dashboards)


Each layer has a clear responsibility and is treated as a contract for the next layer.

---

### 4.2 Star Schema Design

A **star schema** was implemented to support analytical queries efficiently.

#### Fact Tables
- **fact_sales**  
  - Grain: one row per order item  
  - Used for revenue, profit, and conversion analysis

- **fact_refunds**  
  - Grain: one row per refunded order item  
  - Modeled as a secondary fact and applied via measures

#### Dimension Tables
- **dim_product** — product attributes
- **dim_session** — marketing and device context
- **dim_date** — time-based analysis

No fact-to-fact relationships are used.  
All analytical logic is implemented using **measures**, not calculated columns.

---

## 5. Data Transformation

### Python Layer
- Cleaned and validated raw CSV files
- Created curated fact and dimension datasets
- Ensured correct grain and business logic

### Databricks Layer
- Loaded curated datasets as managed tables
- Converted data into **Delta format**
- Treated this layer as a frozen “Gold” analytical contract

This separation ensures scalability and aligns with industry best practices.

---

## 6. Power BI Semantic Model

Power BI was used to build the semantic layer:

- Star schema relationships configured in Model view
- Single-direction filters from dimensions to facts
- Measures created using DAX for all KPIs
- No transformations performed in Power Query

This approach ensures:
- High performance
- Clear business logic
- Maintainable models

---

## 7. Dashboards & Insights

Three dashboards were created:

### 7.1 Executive Overview
Provides a high-level view of:
- Net Sales
- Orders and items sold
- Profit margin
- Conversion rate
- Sales trends by product and device

**Key insight:**  
Desktop users contribute the majority of revenue, despite fewer sessions than mobile users.

---

### 7.2 Marketing Performance
Analyzes marketing efficiency:
- Sessions, conversion rate, and revenue per session
- Performance by UTM source and campaign
- Device-level segmentation

**Key insight:**  
Paid search drives most revenue, while certain campaigns show higher conversion efficiency despite lower traffic.

---

### 7.3 Product & Refund Analysis
Focuses on product-level performance:
- Net sales by product
- Refund amounts and refund rates
- Identification of high-risk products

**Key insight:**  
Some high-revenue products also have elevated refund rates, indicating potential quality or expectation issues.

---

## 8. Key KPIs Implemented

- Total Sales
- Net Sales
- Total Orders
- Total Items Sold
- Total Sessions
- Conversion Rate %
- Revenue per Session
- Total Refunds
- Refund Rate %
- Profit Margin %

All KPIs are documented separately in `KPIs.md`.

---

## 9. Challenges & Learnings

- Correctly identifying **data grain** was critical to avoid misleading results
- Refunds required careful modeling to avoid fact-to-fact relationships
- Databricks Community Edition constraints required adapting ingestion strategies
- Clear separation of responsibilities across layers improved reliability

These challenges reflect real-world analytics problems rather than textbook scenarios.

---

## 10. Limitations & Future Enhancements

### Current Limitations
- Static historical data
- Import-based Power BI refresh
- No predictive analytics

### Future Enhancements
- Customer lifetime value analysis
- Cohort and retention analysis
- Migration to cloud-native storage (Azure Data Lake)
- DirectQuery implementation with Azure Databricks
- Automated refresh pipelines

---

## 11. Conclusion

This project demonstrates the design and implementation of a **production-style analytics solution**, from raw data to executive dashboards.

It highlights:
- Strong data modeling fundamentals
- Clear separation of analytical layers
- Business-focused KPI design
- Effective storytelling through dashboards

The solution provides Maven Fuzzy Factory with a scalable foundation for data-driven decision-making.
