# Data Warehouse Implementation and Data Cleansing, MVP

![1200px-Deloitte svg](https://github.com/Mohammad00197/MVP/assets/60831927/efdfd011-c15b-4ea0-8ba5-fc08e1e494f2)

## Table of Contents
- [Project Overview](#project-overview)
  - [Scripts](#scripts)
  - [Approach](#approach)
- [How to run the scripts](#how-to-run-the-scripts)
- [Data Quality Inconsistencies](#data-quality-inconsistencies)
- [Normalization Standards](#normalization-standards)
- [Deliverables](#deliverables)


## Project Overview
This project aims to design and implement a data warehouse in alignment with MVP financial reporting requirements. MVP requires automated data cleansing processes and high-quality data from transactional files. The project is designed to align with global data management standards to ensure data consistency, accuracy, and readiness for descriptive analysis.



### Scripts
- **pipeline_scr.py**: The main script for running the ETL (Extract, Transform, Load) pipeline, including data extraction, transformation, and loading processes.
- **data_quality_helpers.py**: Contains helper functions used for data quality checks and cleansing operations.



### Approach
My role involves designing and implementing an ELT (Extract, Load, Transform) pipeline and data warehouse for MVP, which sends data on a monthly basis through SFTP. These files can be modified and updated post-delivery, necessitating robust data processing capabilities. I have crafted a high-level design and technical architecture to address this.

The data pipeline begins with the extraction of raw data files from SFTP, followed by data quality checks and cleansing using Python scripts. These scripts address inconsistencies such as missing values, duplicate records, and schema validation issues. The cleansed data is then loaded into a local data warehouse setup. Although the data is currently processed locally, the baseline code structure is designed to be scalable and ready for integration with cloud storage solutions in the future.

The high-level design includes various layers in the data warehouse for different data domains, such as product master data, customer master data, and transactional data. Automated data ingestion and transformation processes ensure data quality and compliance with global standards. The final step involves generating data quality reports and preparing data for descriptive analytics.

This repository contains scripts for running the ETL pipeline, performing data quality checks, and generating reports, ensuring that the data meets the high-quality standards required for accurate financial reporting and analysis.

Why chosing Google Cloud Platform?
BigQuery is a serverless, scalable, and cost-effective cloud data warehouse for fast analysis of large datasets. 

Key components include:

- **Storage**: Data stored in a columnar format on Google's Colossus file system, optimized for analytical queries and scalable.
- **Compute**: Uses the Dremel Engine for parallel query processing, enabling quick handling of complex queries.
- **API**: Offers a rich API for querying, loading data, and managing datasets.

The architecture separates storage and compute, allowing independent scaling, unlike traditional data warehouses. Being serverless, there’s no infrastructure to manage.

Additional benefits:

- **High availability**: 99.9% uptime SLA.
- **Security**: Features like encryption and role-based access control.
- **Cost-effectiveness**: Pricing based on data storage and query usage.

BigQuery is ideal for businesses needing rapid and cost-effective analysis of large datasets.



### Data Quality Inconsistencies

The helpers script identified and addressed various data quality inconsistencies to ensure high-quality data for analysis. Here are the key types of inconsistencies I encountered and the measures taken to resolve them:

**Duplicate Records:** Identified and removed duplicate records both within individual files and across multiple files to prevent inaccurate analyses.

**Missing Values:** Detected columns with missing values and implemented strategies to handle these missing entries, ensuring completeness of data.

**Schema Validation Issues:** Ensured all columns adhered to the defined schema, addressing issues such as mixed data types within a column and missing columns.

**Price Inconsistencies:** Checked for significant deviations in price per unit across similar contexts and flagged these as inconsistencies for further review.

**Inconsistent Associations:** Verified associations between entities (e.g., Product ID and Product Name, Customer ID and Customer Name) to ensure each entity was consistently represented across the dataset.

**File Format and Taxonomy Issues:** Ensured that all files followed the correct format and taxonomy as defined in the project requirements.

**Null Values:** Implemented checks to identify and handle null values in critical columns to maintain data integrity.

**Data Type Consistency:** Ensured consistent data types within columns to prevent errors during analysis and reporting.

These inconsistencies were automatically detected and reported by the data quality scripts. The implemented controls and remediation steps have significantly improved the reliability and accuracy of the data, ensuring it meets the high standards required for MVP's financial reporting and descriptive analysis.




## Normalization Standards

To ensure the data is optimally structured for analysis, the project adheres to the NF3 (Third Normal Form) standards of normalization. These standards help in organizing the data to reduce redundancy, improve data integrity, and enhance query performance. Here are the key aspects of the normalization process followed in this project:

**Elimination of Redundant Data**: Data is organized into separate tables based on entities and their relationships, minimizing duplication of data across the database.

**Data Integrity**: By structuring data in a normalized format, we ensure that data dependencies are logical and consistent. This prevents anomalies during data insertion, update, or deletion.

**Separation of Entities**: Each table contains data related to a single entity, such as Customers, Products, Orders, and Regions. This clear separation makes it easier to manage and maintain the data.

**Foreign Keys**: Relationships between tables are established using foreign keys. This helps in maintaining referential integrity and ensures that related data across tables remains consistent.

**Hierarchical Data Organization**: Data is organized hierarchically, allowing for efficient querying and reporting. This structure supports complex queries needed for descriptive analytics.

**Enhanced Query Performance**: By following NF3 standards, the database structure is optimized for performance, enabling faster retrieval and analysis of data.






## Deliverables

| **Deliverable**                          | **Description**                                                                                           | **File/Document**                          |
|------------------------------------------|-----------------------------------------------------------------------------------------------------------|--------------------------------------------|
| ETL Pipeline Scripts                     | Automates the extraction, transformation, and loading of data.                                             | `pipeline_scr.py`, `data_quality_helpers.py` |
| Data Quality Reports                     | Automatically generated reports highlighting data quality issues, including inconsistencies and errors.    | `data_quality_report.csv`                  |
| High-Level Design and Technical Architecture | Documentation detailing the high-level design and technical architecture of the data warehouse.             | `Task_2_HLD.pptx`                          |
| Cleansed and Normalized Data             | High-quality, normalized data ready for analysis.                                                          | Data files in `cleansed_data/` directory   |
| Business Glossary                        | Definitions and descriptions for key performance indicators (KPIs).                                        | `Business_Glossary.csv`                    |
| Data Quality Check Scripts               | Helper scripts for performing data quality checks and cleansing operations.                                | `data_quality_helpers.py`                  |
| Data Warehouse Structure Documentation   | Detailed documentation on the structure and organization of the data warehouse.                            | Included in `Task_2_HLD.pptx`              |
| Normalization Standards Documentation    | Explanation and implementation details of NF3 normalization standards.                                     | Included in `Task_2_HLD.pptx`              |
| Data Ingestion and Processing Flow       | Documentation of the end-to-end data ingestion and processing flow.                                         | Included in `Task_2_HLD.pptx`              |

## How to run the scripts


1. **Clone the repository**:
   ```bash
   git clone https://github.com/Mohammad00197/MVP.git
   cd MVP
   
2. **Ensure you have Python installed. Then, install the necessary packages using pip and run the pipline**:
  ```bash
  pip install -r requirements.txt
  python pipeline_scr.py





