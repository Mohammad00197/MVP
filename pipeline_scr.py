import os
import shutil
from data_quality_helpers import *

standard_schema = {
    "Row ID": int,
    "Order ID": str,
    "Order Date": str,
    "Ship Date": str,
    "Ship Mode": str,
    "Customer ID": str,
    "Customer Name": str,
    "Segment": str,
    "Country": str,
    "City": str,
    "State": str,
    "Postal Code": str,
    "Region": str,
    "Product ID": str,
    "Category": str,
    "Sub-Category": str,
    "Product Name": str,
    "Sales": float,
    "Quantity": int,
    "Discount": float,
    "Profit": float
}

base_file_path = os.listdir()
csv_files = [file for file in base_file_path if file.endswith('.csv')]

report, issue_examples, cleaned_order_details, inconsistencies_to_check = analyze_files(csv_files, standard_schema)

save_report_to_excel(report, issue_examples)

data_marts = create_data_marts(cleaned_order_details)  
save_data_marts_to_csv(data_marts)

data_mart_counts_df = summarize_data_marts(data_marts)
save_summary_to_csv(data_mart_counts_df)

shutil.make_archive('Task_6_1_Data_Marts', 'zip', 'data_marts')

inconsistencies_to_check.to_csv('inconsistencies_to_check_with_SEMS.csv', index=False)

print("All tasks are completed successfully.")
