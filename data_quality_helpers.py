import pandas as pd
import chardet
import os
import re

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def load_data(file_path):
    encoding = detect_encoding(file_path)
    return pd.read_csv(file_path, encoding=encoding, delimiter='|', on_bad_lines='skip')

def files_extension_check(base_files):
    issues = []
    for file in base_files:
        if not file.endswith(".csv"):
            issues.append(f"Invalid file extension: {file}")
    return issues

def check_file_name_taxonomy(file_name):
    pattern = r'^\d{6}_Orders_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}\.csv$'
    if not re.match(pattern, file_name):
        return f"Invalid file name format: {file_name}"
    return None

def validate_schema(df, schema):
    validation_errors = []
    for column, expected_type in schema.items():
        if column not in df.columns:
            validation_errors.append(f"Missing column: {column}")
        else:
            try:
                df[column] = df[column].astype(expected_type)
            except ValueError as e:
                validation_errors.append(f"Error converting column {column} to {expected_type}: {e}")
    for column in df.columns:
        if column not in schema and column != 'Source File':
            validation_errors.append(f"Unexpected column: {column}")
    return validation_errors

def check_null_values(df):
    null_issues = []
    for column in df.columns:
        if df[column].isnull().any():
            null_issues.append((column, df[df[column].isnull()]))
    return null_issues

def check_duplicates(df, subset_columns):
    return df[df.duplicated(subset=subset_columns, keep=False)]

def check_inconsistent_associations(df):
    inconsistent_customer_ids = df.groupby('Customer ID')['Customer Name'].nunique()
    inconsistent_customer_ids = inconsistent_customer_ids[inconsistent_customer_ids > 1]
    inconsistent_product_ids  = df.groupby('Product ID')['Product Name'].nunique()
    inconsistent_product_ids  = inconsistent_product_ids[inconsistent_product_ids > 1]
    return inconsistent_customer_ids,inconsistent_product_ids

def check_mixed_data_types(df):
    mixed_type_issues = []
    for column in df.columns:
        types = df[column].map(type).unique()
        if len(types) > 1:
            mixed_type_issues.append((column, types))
    return mixed_type_issues

def calculate_price_per_unit(df):
    try:
        df['Price per Unit'] = df.apply(lambda row: row['Sales'] / row['Quantity'] if row['Sales'] != 0 and row['Quantity'] != 0 else None, axis=1)
    except KeyError as e:
        print(f"Missing column during price calculation: {e}")
    return df

def find_price_inconsistencies(df, golden_records):
    inconsistencies = []
    for index, row in df.iterrows():
        key = (row.get('Product ID'),row.get('Ship Mode'),row.get('Customer ID'),row.get('Customer Name'),row.get('Segment'),row.get('Country'),row.get('City'),row.get('State'),row.get('Postal Code'),row.get('Region'))
        price_per_unit = row.get('Price per Unit')
        if key in golden_records:
            avg_price = golden_records[key]
            if price_per_unit is not None and abs(price_per_unit - avg_price) / avg_price > 0.1:
                inconsistencies.append(row)
    return pd.DataFrame(inconsistencies)

def check_sales_profit_inconsistencies(df):
    try:
        return df[(df['Sales'] == 0) & (df['Profit'] != 0)]
    except KeyError:
        return pd.DataFrame()

def analyze_files(base_files, standard_schema):
    report = {
        "File Extension Issues": files_extension_check(base_files),
        "File Name Issues": [],
        "Schema Validation Issues": [],
        "Null Value Issues": [],
        "Duplicate Issues": [],
        "Cross File Duplicate Issues": [],
        "Inconsistent Associations Issues": [],
        "Mixed Data Type Issues": [],
        "Price Inconsistencies": [],
        "Sales Profit Inconsistencies": [],
        "Missing Columns": []
    }
    
    all_data = pd.DataFrame()
    all_data['Source File'] = pd.Series(dtype='str')
    
    issue_examples = {
        "File Name Issues": [],
        "Schema Validation Issues": [],
        "Null Value Issues": [],
        "Duplicate Issues": [],
        "Inconsistent Customer ID Issues": [],
        "Inconsistent Product ID Issues": [],
        "Cross File Duplicate Issues": [],
        "Mixed Data Type Issues": [],
        "Price Inconsistencies": [],
        "Sales Profit Inconsistencies": [],
        "Missing Columns": []
    }
    
    for file in base_files:
        if file.endswith(".csv"):
            file_name_issue = check_file_name_taxonomy(file)
            if file_name_issue:
                report["File Name Issues"].append((file, file_name_issue, "Verify file name format"))
                issue_examples["File Name Issues"].append((file, file_name_issue))
                continue

            df = load_data(file)
            df['Source File'] = file
            schema_issues = validate_schema(df, standard_schema)
            null_issues = check_null_values(df)

            missing_columns = [col for col in standard_schema if col not in df.columns]
            if missing_columns:
                report["Missing Columns"].append((file, f"Missing columns: {' , '.join(missing_columns)}", len(df), "Ensure all required columns are present"))
                issue_examples["Missing Columns"].append((file, {"Missing Columns": missing_columns}))
                continue

            subset_columns = ['Order ID', 'Product ID', 'Customer ID', 'Order Date', 'Ship Date', 'Country', 'Sales', 'Quantity', 'Discount', 'Profit']
            duplicates = check_duplicates(df, subset_columns)
            inconsistent_customer_ids, inconsistent_product_ids = check_inconsistent_associations(df)
            mixed_type_issues = check_mixed_data_types(df)
            df = calculate_price_per_unit(df)
            
            for issue in schema_issues:
                report["Schema Validation Issues"].append((file, issue, len(df), "Verify and correct data types"))
                issue_examples["Schema Validation Issues"].append(df)
            for column, null_rows in null_issues:
                report["Null Value Issues"].append((file, f"Null values in column {column}", len(null_rows), "Fill or remove null values"))
                issue_examples["Null Value Issues"].append(null_rows)
            if not duplicates.empty:
                report["Duplicate Issues"].append((file, "Found duplicates", len(duplicates), "Remove duplicate rows"))
                issue_examples["Duplicate Issues"].append(duplicates)
            if not inconsistent_customer_ids.empty:
                report["Inconsistent Associations Issues"].append((file, "Customer ID associated with multiple Customer Names", len(inconsistent_customer_ids), "Ensure unique Customer Names for each Customer ID"))
                issue_examples["Inconsistent Customer ID Issues"].append(inconsistent_customer_ids)
            if not inconsistent_product_ids.empty:
                report["Inconsistent Associations Issues"].append((file, "Product ID associated with multiple Product Names", len(inconsistent_product_ids), "Ensure unique Product Names for each Product ID"))
                inconsistent_products = df[df['Product ID'].isin(inconsistent_product_ids.index)]
                issue_examples["Inconsistent Product ID Issues"].append(inconsistent_products)
            if mixed_type_issues:
                report["Mixed Data Type Issues"].append((file, "Found mixed data types", len(mixed_type_issues), "Ensure consistent data types within each column"))
                issue_examples["Mixed Data Type Issues"].append(pd.DataFrame(mixed_type_issues, columns=["Column", "Data Types"]))

            all_data = pd.concat([all_data, df], ignore_index=True)

    context_columns = ['Product ID', 'Ship Mode', 'Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region']
    golden_records = all_data.groupby(context_columns)['Price per Unit'].mean().to_dict()

    price_inconsistencies = find_price_inconsistencies(all_data, golden_records)
    if not price_inconsistencies.empty:
        report["Price Inconsistencies"].append(("Multiple files", "Found price inconsistencies", len(price_inconsistencies), "Ensure consistent pricing per product within context"))
        issue_examples["Price Inconsistencies"].append(price_inconsistencies)

    sales_profit_inconsistencies = check_sales_profit_inconsistencies(all_data)
    if not sales_profit_inconsistencies.empty:
        report["Sales Profit Inconsistencies"].append(("Multiple files", "Found sales and profit inconsistencies", len(sales_profit_inconsistencies), "Verify sales and profit data"))
        issue_examples["Sales Profit Inconsistencies"].append(sales_profit_inconsistencies)

    cross_file_duplicates = all_data[all_data.duplicated(subset=subset_columns, keep=False)]
    if not cross_file_duplicates.empty:
        cross_file_duplicates = cross_file_duplicates.copy()
        cross_file_duplicates['Duplicate'] = True
        report["Cross File Duplicate Issues"].append(("Multiple files", "Found cross-file duplicates", len(cross_file_duplicates), "Remove duplicate rows across files"))
        cross_file_duplicates = cross_file_duplicates.sort_values(by="Order ID")
        issue_examples["Cross File Duplicate Issues"].append(cross_file_duplicates)
    
    duplicated_order_details = all_data[all_data.duplicated(subset=['Order ID', 'Product ID'], keep=False)]
    zero_sales_and_non_zero_sales = duplicated_order_details.groupby(['Order ID', 'Product ID']).filter(lambda x: (x['Sales'] == 0).any() and (x['Sales'] > 0).any())
    all_duplicates_check = duplicated_order_details.groupby(['Order ID', 'Product ID']).apply(lambda x: (x['Sales'] == 0).any() and (x['Sales'] > 0).any()).reset_index(name='Zero and Non-Zero Sales')
    other_duplicated_issues = duplicated_order_details.groupby(['Order ID', 'Product ID']).filter(lambda x: not ((x['Sales'] == 0).any() and (x['Sales'] > 0).any()))

    all_data['inconsistency_to_be_checked_with_SEMS'] = all_data.apply(
        lambda row: (row['Order ID'], row['Product ID']) in other_duplicated_issues[['Order ID', 'Product ID']].values, axis=1
    )
    
    # Remove records with zero sales and zero quantity
    cleaned_order_details = all_data[~((all_data['Sales'] == 0) & (all_data['Quantity'] == 0))]
    
    inconsistencies_to_check = all_data[all_data['inconsistency_to_be_checked_with_SEMS']]
    
    return report, issue_examples, cleaned_order_details, inconsistencies_to_check

def save_report_to_excel(report, issue_examples, file_name='Task_5_Inconsistencies_Analysis.xlsx'):
    summary_report = []
    for issue_type, issues in report.items():
        distinct_row_ids = set()
        if issue_type in issue_examples:
            for example in issue_examples[issue_type]:
                if isinstance(example, pd.DataFrame) and 'Row ID' in example.columns:
                    distinct_row_ids.update(example['Row ID'].unique())
        distinct_count = len(distinct_row_ids)
        for issue in issues:
            if isinstance(issue, tuple) and len(issue) == 4:
                file, description, affected_rows, resolution = issue
                summary_report.append([issue_type, description, resolution, distinct_count])

    summary_df = pd.DataFrame(summary_report, columns=["Inconsistency Type", "Description", "Suggestion to handle", "Distinct Count of [Row Id]"])

    with pd.ExcelWriter(file_name) as writer:
        summary_df.to_excel(writer, sheet_name='Inconsistencies_Summary', index=False)

        examples_data = []
        for issue_type, examples in issue_examples.items():
            if examples:
                for example in examples:
                    if isinstance(example, pd.DataFrame):
                        example['Inconsistency Type'] = issue_type
                        examples_data.extend(example.to_dict('records'))
                    else:
                        examples_data.append({"Inconsistency Type": issue_type, "Example": str(example)})

        if examples_data:
            df_examples = pd.DataFrame(examples_data)
            df_examples.to_excel(writer, sheet_name='Inconsistencies_Examples', index=False)

        quality_report_data = []
        for issue_type, issues in report.items():
            for issue in issues:
                if isinstance(issue, tuple) and len(issue) == 4:
                    quality_report_data.append({
                        "Issue Type": issue_type,
                        "File": issue[0],
                        "Description": issue[1],
                        "Number of Affected Rows": issue[2],
                        "Plan to Resolve": issue[3]
                    })
        df_quality_report = pd.DataFrame(quality_report_data)
        df_quality_report.to_excel(writer, sheet_name='Quality_Report', index=False)

def create_data_marts(all_data):
    data_marts = {
        "customers": all_data[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates(),
        "products": all_data[['Product ID', 'Category', 'Sub-Category', 'Product Name']].drop_duplicates(),
        "geography": all_data[['Country', 'City', 'State', 'Postal Code', 'Region']].drop_duplicates(),
        "orders": all_data[[
            "Row ID", 'Order ID', 'Order Date', 'Ship Date', 'Ship Mode', 'Customer ID', 
            'Country', 'City', 'State', 'Postal Code', 'Region'
        ]].drop_duplicates(subset=['Order ID', 'Order Date', 'Ship Date', 'Ship Mode', 'Customer ID']),
        "order_details": all_data[['Order ID', 'Product ID', 'Sales', 'Quantity', 'Discount', 'Profit']].drop_duplicates()
    }
    return data_marts


def save_data_marts_to_csv(data_marts, directory='data_marts'):
    os.makedirs(directory, exist_ok=True)
    for mart_name, df in data_marts.items():
        df.to_csv(f'{directory}/{mart_name}.csv', index=False)

def summarize_data_marts(data_marts):
    data_mart_counts = []
    for mart_name, df in data_marts.items():
        row_count = len(df)
        if mart_name == "order_details":
            distinct_order_detail_id_count = df['Order ID'].nunique()
            data_mart_counts.append([mart_name, row_count, distinct_order_detail_id_count, None])
        elif 'Row ID' in df.columns:
            distinct_row_id_count = df['Row ID'].nunique()
            data_mart_counts.append([mart_name, row_count, row_count, distinct_row_id_count])
        else:
            data_mart_counts.append([mart_name, row_count, row_count, None])
    data_mart_counts_df = pd.DataFrame(data_mart_counts, columns=["Data Mart System Name", "Count Rows", "Count Distinct Primary Key", "Count Distinct Row ID"])
    return data_mart_counts_df

def save_summary_to_csv(data_mart_counts_df, file_name='Task_6_2_Data_Marts_Rows.csv'):
    data_mart_counts_df.to_csv(file_name, index=False)
