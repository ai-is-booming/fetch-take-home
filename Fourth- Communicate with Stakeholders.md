
### 1. What questions do you have about the data?
- **Are all receipts expected to have associated items?** Only 679 of 1,119 receipts link to `items`, leaving 440 without details—does this reflect incomplete scanning or intentional exclusions (e.g., non-itemized receipts)?
- **Why are 55.5% of items (3,851/6,941) missing barcodes?** Is this a data capture issue at the source, or are some items intentionally unscannable (e.g., generic products)?
- **Are the 117 users in `receipts` without `users_dedup` records expected?** Are these missing due to data ingestion gaps, or do they represent external users not tracked in our system?
- **For the 7 duplicate barcodes in `brands`, which brand assignment is correct?** Without additional context, we can’t determine the true brand—does this stem from overlapping product catalogs or errors? Also, instead of relying on barcodes, should we consider using brand_id as the unique identifier for brands? If so, we would need to introduce brand_id to other tables, allowing it to serve as a consistent joining point across the database, rather than relying on barcodes, which may not always be unique or reliable.
- **Why don't we have 'Accepted' in `receipts.rewards_receipt_status`?** One of the predetermined questions askes about 'Accepted', but I don't see this value in rewards_receipt_status. Currently, I am treating 'Finished' as 'Accepted,' but I would like to gather more context to ensure we conduct accurate analytics.

**Context**: These questions emerged from analyzing table relationships and row counts, highlighting gaps and inconsistencies.

---

### 2. How did you discover the data quality issues?
I identified the issues using SQL queries in Redshift Serverless:
- **Duplicates in `users`**: 
  - `SELECT COUNT(1), COUNT(DISTINCT user_id) FROM users` returned 495 rows vs. 212 unique `user_id`s.
  - `SELECT user_id, COUNT(1) FROM users GROUP BY 1 HAVING COUNT(1) > 1` listed specific duplicates (e.g., `5ff5d15aeb7c7d12096d91a2`).
  - A distinct subquery confirmed identical rows across all columns.
- **Duplicate barcodes in `brands`**: 
  - `SELECT COUNT(1), COUNT(DISTINCT brand_id), COUNT(DISTINCT barcode)` showed 1167 rows, 1167 `brand_id`s, but only 1160 `barcode`s.
  - `SELECT barcode, COUNT(1) FROM brands GROUP BY 1 HAVING COUNT(1) > 1` identified 7 duplicates.
- **Incomplete Linkages**: 
  - `receipts` vs. `items`: `LEFT JOIN` revealed 1119 distinct `receipt_id`s in `receipts`, only 679 in `items`.
  - `receipts` vs. `users_dedup`: 258 distinct `user_id`s in `receipts`, only 141 in `users_dedup`.
  - `items` vs. `brands_cleaned`: 568 distinct `barcode`s in `items`, only 15 in `brands_cleaned`.
- **Missing Barcodes in `items`**: 
  - `SELECT COUNT(CASE WHEN barcode IS NOT NULL THEN 1 END) FROM items` found 3090 non-null vs. 6941 total rows.
- **Missing 'Accepted' in rewardsReceiptStatus**: 
  - `SELECT rewards_receipt_status, AVG(total_spent) average_spent FROM receipts GROUP BY 1 ORDER BY 1` showed no 'Accepted' in the results.

**Method**: Systematic SQL validation (counts, joins, grouping) during table creation and analysis exposed these issues.

---

### 3. What do you need to know to resolve the data quality issues?
- **Duplicates**:
  - **Users**: Was duplication intentional (e.g., multiple sign-ups) or an ETL error? Need source system logs or pipeline details.
  - **Brands**: Which brand is correct for each of the 7 duplicate barcodes? Requires SME input or a reference catalog. Alternatively, after consulting with an SME, should we consider using brand_id as the primary identifier instead of barcodes?
- **Incomplete Linkages**:
  - **Receipts to Items**: Are the 440 unlinked receipts valid without items? Need business rules on receipt completeness.
  - **Receipts to Users**: Are the 117 missing users external or lost data? Need user registration process details or a full user export.
  - **Items to Brands**: Why do only 15 of 568 `items` barcodes match `brands`? Need to confirm if `brands` is a subset or if `items` barcodes are misrecorded.
- **Missing Barcodes**: What’s the process for capturing `barcode` in `items`? Need source system specs or scanning logs.
- **Missing 'Accepted'**: What’s the full list of rewardsReceiptStatus?

**Next Steps**: Stakeholder discussions and pipeline documentation are essential to clarify intent and root causes.

---

### 4. What other information would you need to help you optimize the data assets you're trying to create?
- **Business Context**: What key metrics matter (e.g., top brands, user spend)? This prioritizes fixes (e.g., `barcode` linkage vs. user duplicates).
- **Source Data Pipeline**: How the underlying data for`receipts`, `users`, `items`, and `brands` being generated? Talking to the data engineering team about the data sources could pinpoint duplication or gaps.
- **Data Freshness**: Update frequency (daily, weekly)? Impacts deduplication strategy (incremental vs. full refresh).
- **External References**: A master product catalog or user registry to validate `barcode`s and `user_id`s, filling gaps.
- **Usage Patterns**: Who uses the data and how (dashboards, analytics)? Guides schema optimization (e.g., indexing `barcode`).
- **Historical Data**: Are issues consistent over time? Older data could reveal systemic vs. recent problems.

**Goal**: Optimize joined tables (`receipts`, `users_dedup`, `items`, `brands_cleaned`) for reliable analytics, aligned with business needs.

---

### 5. What performance and scaling concerns do you anticipate in production and how do you plan to address them?
- **Current Scale**: 
  - `receipts`: 1,119 rows
  - `items`: 6,941 rows
  - `users_dedup`: 212 rows
  - `brands_cleaned`: 1,153 rows
  - Small now, but the database anticipates growth.

- **Concerns and Solutions**:
  1. **Join Performance**: 
     - **Issue**: `LEFT JOIN`s (e.g., `receipts` to `items` to `brands_cleaned`) may slow with larger datasets, especially with null-heavy keys.
     - **Plan**: Add sort keys (e.g., `CREATE TABLE receipts (receipt_id VARCHAR(24) SORTKEY, ...)`) and dist keys (e.g., `DISTKEY(receipt_id)`) for even data distribution.
  2. **Duplicate Processing**: 
     - **Issue**: Python/SQL deduplication (e.g., `users`, `brands`) won’t scale to millions of rows.
     - **Plan**: Shift to ETL pipeline with unique constraints or pre-aggregation.
  3. **Data Volume Growth**: 
     - **Issue**: Millions of receipts could increase cloud computing costs and query latency.
     - **Plan**: Partition by `create_date`.
  4. **Missing Data Impact**: 
     - **Issue**: 55.5% null `barcode`s skew joins as volume grows.
     - **Plan**: Add pipeline quality checks (e.g., reject null `barcode`s or flag for review).
  5. **Concurrency**: 
     - **Issue**: Multiple analysts querying could hit cloud service limits.
     - **Plan**: Leverage auto-scaling, tune workload management (WLM).

- **Implementation**:
  - Optimize schema with sort/dist keys now.
  - Automate deduplication/quality in ETL.
  - Monitor performance, scale the cloud service as needed.
  - Validate with stakeholders to prioritize fixing critical gaps.

---
