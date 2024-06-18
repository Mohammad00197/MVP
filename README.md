# Data Warehouse Implementation and Data Cleansing, MVP

![1200px-Deloitte svg](https://github.com/Mohammad00197/MVP/assets/60831927/efdfd011-c15b-4ea0-8ba5-fc08e1e494f2)

## Table of Contents
- [Project Overview](#project-overview)
- [Files Description](#files-description)
  - [Data Files](#data-files)
  - [Scripts](#scripts)
  - [Approach](#Approach)
- [How to Run the Project](#how-to-run-the-project)
- [Data Quality Inconsistencies](#data-quality-inconsistencies)
- [Normalization Standards](#normalization-standards)
- [Deliverables](#deliverables)
- [Contact Information](#contact-information)

## Project Overview
This project aims to design and implement data warehouse in alignment with MVP financial reporting requirements, MVP requires automated data cleansing processes and high-quality data from transactional files. The project is designed to align with global data management standards to ensure data consistency, accuracy, and readiness for descriptive analysis.

## Files Description

### Data Files
- **Business Glossary**: Defined calculations and descriptions for key performance indicators (KPIs).
- **geography.csv**: customer locations data marts.
- **order_details.csv**: Detailed information about each order.
- **orders.csv**: Contains overall order information (Headers).
- **products.csv**: Contains information about the products.

### Scripts
- **pipeline_scr.py**: The main script for running the ETL (Extract, Transform, Load) pipeline, including data extraction, transformation, and loading processes.
- **data_quality_helpers.py**: Contains helper functions used for data quality checks and cleansing operations.

### Approach
- **High Level Design, Technical Architecture, and Data Cleansing Approach**: Analyzes and documents the data quality inconsistencies identified during the project.

## How to Run the Project

1. **Clone the repository**:
   ```bash
   git clone <>
   cd <>
