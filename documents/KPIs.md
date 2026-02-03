# Key Performance Indicators (KPIs)
## Maven Fuzzy Factory — Sales & Marketing Analytics

This document defines the core KPIs used across the Power BI dashboards.  
Each KPI is designed to answer a specific business question and is calculated from curated fact and dimension tables.

---

## 1. Total Sales

**Definition:**  
Total revenue generated from all sold items before refunds.

**Business Question Answered:**  
How much gross revenue is the business generating?

**Why It Matters:**  
This is the primary topline metric used by executives to assess overall business performance.

**Calculation (High-Level):**  
Sum of product-level sales revenue from all order items.

---

## 2. Net Sales

**Definition:**  
Revenue remaining after subtracting refunded amounts.

**Business Question Answered:**  
What is the true revenue after accounting for refunds?

**Why It Matters:**  
Net sales provide a more accurate measure of financial performance than gross sales.

**Calculation (High-Level):**  
Total Sales − Total Refunds

---

## 3. Total Orders

**Definition:**  
Count of unique customer orders placed.

**Business Question Answered:**  
How many purchase transactions occurred?

**Why It Matters:**  
Helps distinguish between revenue growth driven by higher order volume vs higher order value.

**Calculation (High-Level):**  
Distinct count of order identifiers.

---

## 4. Total Items Sold

**Definition:**  
Total number of individual products sold.

**Business Question Answered:**  
How many products were sold in total?

**Why It Matters:**  
Useful for understanding demand at the product level and identifying high-volume SKUs.

**Calculation (High-Level):**  
Count of order items.

---

## 5. Total Sessions

**Definition:**  
Total number of website sessions.

**Business Question Answered:**  
How much traffic is the website receiving?

**Why It Matters:**  
Forms the top of the marketing funnel and is required to evaluate conversion efficiency.

**Calculation (High-Level):**  
Distinct count of website sessions.

---

## 6. Conversion Rate %

**Definition:**  
Percentage of website sessions that resulted in an order.

**Business Question Answered:**  
How effectively does traffic convert into customers?

**Why It Matters:**  
A key metric for evaluating marketing and website performance.

**Calculation (High-Level):**  
Total Orders ÷ Total Sessions

---

## 7. Revenue per Session

**Definition:**  
Average revenue generated per website session.

**Business Question Answered:**  
How valuable is each visit to the website?

**Why It Matters:**  
Combines traffic quality and conversion efficiency into a single metric.

**Calculation (High-Level):**  
Total Sales ÷ Total Sessions

---

## 8. Total Cost

**Definition:**  
Total cost of goods sold for all items.

**Business Question Answered:**  
What is the cost incurred to generate sales?

**Why It Matters:**  
Required for profitability analysis.

**Calculation (High-Level):**  
Sum of cost of goods sold at the product level.

---

## 9. Total Profit

**Definition:**  
Profit generated after subtracting costs from sales.

**Business Question Answered:**  
Is the business generating profit from its sales?

**Why It Matters:**  
Profitability is critical for long-term sustainability.

**Calculation (High-Level):**  
Total Sales − Total Cost

---

## 10. Profit Margin %

**Definition:**  
Percentage of revenue retained as profit.

**Business Question Answered:**  
How efficient is the business at converting revenue into profit?

**Why It Matters:**  
Helps compare profitability across products, campaigns, and time periods.

**Calculation (High-Level):**  
Total Profit ÷ Total Sales

---

## 11. Total Refunds

**Definition:**  
Total monetary value of refunded items.

**Business Question Answered:**  
How much revenue is lost due to refunds?

**Why It Matters:**  
High refund values may indicate product quality or customer satisfaction issues.

**Calculation (High-Level):**  
Sum of refund amounts from refunded order items.

---

## 12. Refund Rate %

**Definition:**  
Percentage of sales value that is refunded.

**Business Question Answered:**  
What proportion of revenue is being returned?

**Why It Matters:**  
Helps identify risky products and operational problems.

**Calculation (High-Level):**  
Total Refunds ÷ Total Sales

---

## KPI Usage Across Dashboards

| Dashboard Page | Key KPIs Used |
|---------------|--------------|
| Executive Overview | Total Sales, Net Sales, Total Orders, Profit Margin %, Conversion Rate |
| Marketing Performance | Total Sessions, Conversion Rate %, Revenue per Session |
| Product & Refund Analysis | Net Sales, Total Refunds, Refund Rate % |

---

## Notes

- All KPIs are implemented as **DAX measures**, not calculated columns.
- Refunds are modeled as a **secondary fact** and applied through measures.
- Time-based analysis uses a dedicated Date dimension.

