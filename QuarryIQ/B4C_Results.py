import pandas as pd
from io import StringIO
import subprocess
import re
import itertools
import os
from django.conf import settings


def format_with_commas(num):
    return "{:,.0f}".format(num)

def B4C_dat_minVC_Base(quarry, dat_file_path):
    # Retrieve dataframes
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
    capacityModes_df_json = quarry.capacityModes_df_json
    capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
    constituentModesBalance_df_json = quarry.constituentModesBalance_df_json
    constituentModesBalance_df = pd.read_json(StringIO(constituentModesBalance_df_json), orient='records')
    productComposition_df_json = quarry.productComposition_df_json
    productComposition_df = pd.read_json(StringIO(productComposition_df_json), orient='records')

    # Get unique elements in dataframes
    unique_modes = capacityModes_df['Modes'].unique()
    unique_constituents = constituentModesBalance_df['Constituent'].unique()
    unique_products = P4PFullSummary_df['Product'].unique()

    # Calculate Variable Costs
    productsVariableCosts_df = P4PFullSummary_df.copy()
    productsVariableCosts_df['Variable_cost'] = (productsVariableCosts_df['ASP'] - productsVariableCosts_df['Agg_levy'] - productsVariableCosts_df['GM']).round(2)
    productsVariableCosts_df = productsVariableCosts_df.drop(columns=['Product_Cluster', 'Production_Volume', 'Sales_Volume', 'ASP', 'Agg_levy', 'GM','TM'])

    # Get Sales Volume
    productsSalesVolume_df = P4PFullSummary_df.copy()

    # Calculate Production Balance
    constituentModesBalance_df["Balance"] = constituentModesBalance_df["Balance"] / 100

    # Calculate Product Composition
    productComposition_df = productComposition_df.drop(columns=['Production_Volume'])
    productComposition_df['Composition'] = productComposition_df['Composition'] / 100

    # Calculate Coefficient
    capacityModes_df['Coefficient'] = capacityModes_df['Coefficient'] / 100

    # Open the .dat file in write mode
    with open(dat_file_path, "w") as f:
        # SET MODES
        f.write("set MODES :=\n")
        for mode in unique_modes:
            f.write(f'"{mode}"\n')
        f.write(";\n\n")

        # SET CONSTITUENTS
        f.write("set CONSTITUENTS :=\n")
        for constituent in unique_constituents:
            f.write(f'"{constituent}"\n')
        f.write(";\n\n")

        # SET PRODUCTS
        f.write("set PRODUCTS :=\n")
        for product in unique_products:
            f.write(f'"{product}"\n')
        f.write(";\n\n")

        # param Throughput
        f.write("param Throughput :=\n")
        for index, row in capacityModes_df.iterrows():
            f.write(f'"{row["Modes"]}"\t{row["Max_capacity"]}\n')
        f.write(";\n\n")

        # param Variable_cost
        f.write("param Variable_cost :=\n")
        for index, row in productsVariableCosts_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Variable_cost"]}\n')
        f.write(";\n\n")

        # param Sales_demand
        f.write("param Sales_demand :=\n")
        for index, row in productsSalesVolume_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Sales_Volume"]}\n')
        f.write(";\n\n")

        # param Production_balance
        f.write("param Production_balance :=\n")
        for index, row in constituentModesBalance_df.iterrows():
            f.write(f'"{row["Constituent"]}"\t"{row["Modes"]}"\t{row["Balance"]}\n')
        f.write(";\n\n")

        # param Product_composition
        f.write("param Product_composition :=\n")
        for index, row in productComposition_df.iterrows():
            f.write(f'"{row["Product"]}"\t"{row["Constituent"]}"\t{row["Composition"]}\n')
        f.write(";\n\n")

        # param Coefficient
        f.write("param Coefficient :=\n")
        for index, row in capacityModes_df.iterrows():
            f.write(f'"{row["Modes"]}"\t{row["Coefficient"]}\n')
        f.write(";\n\n")

def B4C_GLPK_minVC(model_file, dat_file, result_file):
    # Define the command to run glpsol
    command = [settings.GLPK_PATH, '--model', model_file, '--data', dat_file, '--output', result_file]

    # Execute the command
    result = subprocess.run(command, capture_output=True, text=True)

    # Read the .sol file
    with open(result_file, 'r') as sol_file:
        sol_content = sol_file.read()

    # Custom function to handle scientific notation
    def convert_to_float(value):
        try:
            return float(value.replace('D', 'E'))
        except ValueError:
            return None

    # Initialize variables to store data
    variable_data = []

    # Define patterns to identify the start of the constraints and variables sections
    constraints_pattern = re.compile(r'\s*No.\s+Row name\s+St\s+Activity\s+Lower bound\s+Upper bound\s+Marginal')
    variables_pattern = re.compile(r'\s*No.\s+Column name\s+St\s+Activity\s+Lower bound\s+Upper bound\s+Marginal')

    # Identify the start of the variables section
    variables_start = variables_pattern.search(sol_content)
    if variables_start:
        sol_content = sol_content[variables_start.end():]

    # Define a pattern to extract data for variables
    pattern = re.compile(r'(\w+)?\[(.+?)\]\s+(\w+)\s+([\d\.eE+-]+)?\s+([\d\.eE+-]+)?\s+([\d\.eE+-]+)?\s+([\d\.eE+-]+)?(?!\s*\d)')

    # Search for variable data in the .sol file
    matches = pattern.findall(sol_content)
    for match in matches:
        variable_name = match[0]
        key = match[1]
        status = match[2]

        activity = convert_to_float(match[3])
        lower_bound = convert_to_float(match[4])
        upper_bound = convert_to_float(match[5])
        marginal = convert_to_float(match[6])

        index_parts = key.split(',')
        index1 = index_parts[0].strip() if len(index_parts) >= 1 else ''
        index2 = index_parts[1].strip() if len(index_parts) >= 2 else ''
        index3 = index_parts[2].strip() if len(index_parts) >= 3 else ''
        index4 = index_parts[3].strip() if len(index_parts) >= 4 else ''
        index5 = index_parts[4].strip() if len(index_parts) >= 5 else ''

        variable_data.append({
            'Variable': variable_name,
            'index1': index1,
            'index2': index2,
            'index3': index3,
            'index4': index4,
            'index5': index5,
            'Value': activity if activity is not None else 0,
        })


    # Convert the data to a Pandas DataFrame for further analysis or export
    B4C_dat_minVC_results_df = pd.DataFrame(variable_data)
    B4C_dat_minVC_results_df = B4C_dat_minVC_results_df.drop(columns=['index2', 'index3', 'index3', 'index4', 'index5'])
    return B4C_dat_minVC_results_df

def B4C_Create_minVC_Base_Table(quarry, B4C_dat_minVC_results_df):
    # Retrieve dataframes
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')

    stock_Variation_df = B4C_dat_minVC_results_df[B4C_dat_minVC_results_df['Variable'] == 'Stock_variation'].copy()
    stock_Variation_df = stock_Variation_df.drop(columns=['Variable'])
    stock_Variation_df = stock_Variation_df.rename(columns={'index1': 'Product', 'Value': 'Min_Stock'})
    stock_Variation_df['Product'] = stock_Variation_df['Product'].str.replace("'","")
    stock_Variation_df['Min_Stock'] = stock_Variation_df['Min_Stock'].astype(int)

    minVC_Base_Table_df = P4PFullSummary_df.drop(columns=['Product_Cluster', 'Production_Volume', 'TM']).copy()
    minVC_Base_Table_df['Variable_Cost_per_t'] = minVC_Base_Table_df['ASP'] - minVC_Base_Table_df['Agg_levy'] - minVC_Base_Table_df['GM']
    minVC_Base_Table_df['Turnover'] = minVC_Base_Table_df['Sales_Volume'] * minVC_Base_Table_df['ASP']

    minVC_Base_Table_df = pd.merge(minVC_Base_Table_df, stock_Variation_df, on='Product', how='left')
    minVC_Base_Table_df['Production_Volume'] = minVC_Base_Table_df['Sales_Volume'] + minVC_Base_Table_df['Min_Stock']
    minVC_Base_Table_df['Variable_Cost'] = minVC_Base_Table_df['Production_Volume'] * minVC_Base_Table_df['Variable_Cost_per_t']
    minVC_Base_Table_df['Gross_Margin'] = minVC_Base_Table_df['Turnover'] - minVC_Base_Table_df['Variable_Cost']

    # Calculate the sum of each column
    sum_row_minVC_Base_Table = minVC_Base_Table_df.sum(axis=0)
    sum_row_minVC_Base_Table = sum_row_minVC_Base_Table.drop(['Product'])
    sum_row_minVC_Base_Table_Delta = sum_row_minVC_Base_Table.copy()

    # Format data
    minVC_Base_Table_df['Sales_Volume'] = minVC_Base_Table_df['Sales_Volume'].apply(format_with_commas)
    minVC_Base_Table_df['Min_Stock'] = minVC_Base_Table_df['Min_Stock'].apply(format_with_commas)
    minVC_Base_Table_df['Turnover'] = minVC_Base_Table_df['Turnover'].apply(format_with_commas)
    minVC_Base_Table_df['Variable_Cost'] = minVC_Base_Table_df['Variable_Cost'].apply(format_with_commas)
    minVC_Base_Table_df['Gross_Margin'] = minVC_Base_Table_df['Gross_Margin'].apply(format_with_commas)
    minVC_Base_Table_df['ASP'] = minVC_Base_Table_df['ASP'].round(2)
    minVC_Base_Table_df['Variable_Cost_per_t'] = minVC_Base_Table_df['Variable_Cost_per_t'].round(2)

    sum_row_minVC_Base_Table = sum_row_minVC_Base_Table.apply(format_with_commas)

    return minVC_Base_Table_df, sum_row_minVC_Base_Table, sum_row_minVC_Base_Table_Delta

def B4C_Create_Download_minVC(project_data, quarry):
    # Retrieve dataframes
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')

    # Build customersSalesASPProduct_df
    customersSalesASPProduct_df = modified_transactions_df[
        ['Quarries', 'Customers', 'Sales_Volume', 'Product', 'Average sales price']].copy()
    customersSalesASPProduct_df = customersSalesASPProduct_df[customersSalesASPProduct_df['Quarries'] == quarry.name]
    customersSalesASPProduct_df['TO'] = customersSalesASPProduct_df['Sales_Volume'] * customersSalesASPProduct_df['Average sales price']
    customersSalesASPProduct_df = customersSalesASPProduct_df.drop(columns=['Quarries', 'Average sales price'])
    customersSalesASPProduct_df['Customers'] = customersSalesASPProduct_df['Customers'].str.replace("'", "")
    customersSalesASPProduct_df = customersSalesASPProduct_df.groupby(['Customers', 'Product']).agg(
        {'Sales_Volume': 'sum', 'TO': 'sum'}).reset_index()
    customersSalesASPProduct_df['ASP'] = customersSalesASPProduct_df['TO'] / customersSalesASPProduct_df['Sales_Volume']

    # Calculate Variable Costs
    productsVariableCosts_df = P4PFullSummary_df.copy()
    productsVariableCosts_df['Variable_Cost_per_t'] = (productsVariableCosts_df['ASP'] - productsVariableCosts_df['Agg_levy'] - productsVariableCosts_df['GM']).round(2)
    productsVariableCosts_df = productsVariableCosts_df.drop(columns=['Product_Cluster', 'Production_Volume', 'Sales_Volume', 'ASP', 'Agg_levy', 'GM', 'TM'])

    # Prepare minVC_Base_Table_df_Downaload
    minVC_Base_Table_df_Downaload = pd.merge(customersSalesASPProduct_df, productsVariableCosts_df, on='Product', how='left')
    minVC_Base_Table_df_Downaload['Turnover'] = minVC_Base_Table_df_Downaload['ASP'] * minVC_Base_Table_df_Downaload['Sales_Volume']
    minVC_Base_Table_df_Downaload['Gross_Margin_per_t'] = minVC_Base_Table_df_Downaload['ASP'] - minVC_Base_Table_df_Downaload['Variable_Cost_per_t']

    columns_order = ['Customers', 'Product', 'Sales_Volume', 'ASP', 'Turnover', 'Variable_Cost_per_t', 'Gross_Margin_per_t']
    minVC_Base_Table_df_Downaload = minVC_Base_Table_df_Downaload[columns_order]

    return minVC_Base_Table_df_Downaload


def B4C_dat_maxGM_Base(quarry, project_data, dat_file_path):
    # Retrieve dataframes
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
    capacityModes_df_json = quarry.capacityModes_df_json
    capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
    constituentModesBalance_df_json = quarry.constituentModesBalance_df_json
    constituentModesBalance_df = pd.read_json(StringIO(constituentModesBalance_df_json), orient='records')
    productComposition_df_json = quarry.productComposition_df_json
    productComposition_df = pd.read_json(StringIO(productComposition_df_json), orient='records')
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Build customersSalesASPProduct_df
    customersSalesASPProduct_df = modified_transactions_df[['Quarries', 'Customers', 'Sales_Volume', 'Product', 'Average sales price']].copy()
    customersSalesASPProduct_df = customersSalesASPProduct_df[customersSalesASPProduct_df['Quarries'] == quarry.name]
    customersSalesASPProduct_df = customersSalesASPProduct_df.rename(columns={'Average sales price': 'ASP'})
    customersSalesASPProduct_df = customersSalesASPProduct_df.drop(columns=['Quarries'])
    customersSalesASPProduct_df['Customers'] = customersSalesASPProduct_df['Customers'].str.replace("'","")

    # Get unique elements in dataframes
    unique_modes = capacityModes_df['Modes'].unique()
    unique_constituents = constituentModesBalance_df['Constituent'].unique()
    unique_products = P4PFullSummary_df['Product'].unique()
    customersSalesASPProduct_df['Customers'] = customersSalesASPProduct_df['Customers'].str.replace('"',"")
    unique_customers = customersSalesASPProduct_df['Customers'].unique()

    # Generate all combinations of customers and products
    combinations = list(itertools.product(unique_customers, unique_products))
    # Create DataFrame from combinations
    temp_df = pd.DataFrame(combinations, columns=['Customers', 'Product'])
    # Perform left merge
    customersSalesASPProduct_df = pd.merge(temp_df, customersSalesASPProduct_df, on=['Customers', 'Product'], how='left')
    # Fill missing values with 0
    customersSalesASPProduct_df['Sales_Volume'] = customersSalesASPProduct_df['Sales_Volume'].fillna(0)
    customersSalesASPProduct_df['ASP'] = customersSalesASPProduct_df['ASP'].fillna(0)

    customersSalesASPProduct_df['TO'] = customersSalesASPProduct_df['Sales_Volume'] * customersSalesASPProduct_df['ASP']
    customersSalesASPProduct_df = customersSalesASPProduct_df.drop(columns=['ASP'])
    customersSalesASPProduct_df = customersSalesASPProduct_df.groupby(['Customers', 'Product']).agg(
        {'Sales_Volume': 'sum', 'TO': 'sum'}).reset_index()
    customersSalesASPProduct_df['ASP'] = customersSalesASPProduct_df['TO'] / customersSalesASPProduct_df['Sales_Volume']
    customersSalesASPProduct_df['ASP'] = customersSalesASPProduct_df['ASP'].fillna(0)


    # Calculate Variable Costs
    productsVariableCosts_df = P4PFullSummary_df.copy()
    productsVariableCosts_df['Variable_cost'] = (productsVariableCosts_df['ASP'] - productsVariableCosts_df['Agg_levy'] - productsVariableCosts_df['GM']).round(2)
    productsVariableCosts_df = productsVariableCosts_df.drop(columns=['Product_Cluster', 'Production_Volume', 'Sales_Volume', 'Agg_levy', 'GM','TM'])

    # Calculate Production Balance
    constituentModesBalance_df["Balance"] = constituentModesBalance_df["Balance"] / 100

    # Calculate Product Composition
    productComposition_df = productComposition_df.drop(columns=['Production_Volume'])
    productComposition_df['Composition'] = productComposition_df['Composition'] / 100

    # Calculate Coefficient
    capacityModes_df['Coefficient'] = capacityModes_df['Coefficient'] / 100


    # Open the .dat file in write mode
    with open(dat_file_path, "w") as f:
        # SET MODES
        f.write("set MODES :=\n")
        for mode in unique_modes:
            f.write(f'"{mode}"\n')
        f.write(";\n\n")

        # SET CONSTITUENTS
        f.write("set CONSTITUENTS :=\n")
        for constituent in unique_constituents:
            f.write(f'"{constituent}"\n')
        f.write(";\n\n")

        # SET PRODUCTS
        f.write("set PRODUCTS :=\n")
        for product in unique_products:
            f.write(f'"{product}"\n')
        f.write(";\n\n")

        # SET CUSTOMERS
        f.write("set CUSTOMERS :=\n")
        for customer in unique_customers:
            f.write(f'"{customer}"\n')
        f.write(";\n\n")

        # param Throughput
        f.write("param Throughput :=\n")
        for index, row in capacityModes_df.iterrows():
            f.write(f'"{row["Modes"]}"\t{row["Max_capacity"]}\n')
        f.write(";\n\n")

        # param Variable_cost
        f.write("param Variable_cost :=\n")
        for index, row in productsVariableCosts_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Variable_cost"]}\n')
        f.write(";\n\n")

        # param Sales_demand
        f.write("param Sales_demand :=\n")
        for index, row in customersSalesASPProduct_df.iterrows():
            f.write(f'"{row["Customers"]}"\t"{row["Product"]}"\t{row["Sales_Volume"]}\n')
        f.write(";\n\n")

        # param Production_balance
        f.write("param Production_balance :=\n")
        for index, row in constituentModesBalance_df.iterrows():
            f.write(f'"{row["Constituent"]}"\t"{row["Modes"]}"\t{row["Balance"]}\n')
        f.write(";\n\n")

        # param Product_composition
        f.write("param Product_composition :=\n")
        for index, row in productComposition_df.iterrows():
            f.write(f'"{row["Product"]}"\t"{row["Constituent"]}"\t{row["Composition"]}\n')
        f.write(";\n\n")

        # param Coefficient
        f.write("param Coefficient :=\n")
        for index, row in capacityModes_df.iterrows():
            f.write(f'"{row["Modes"]}"\t{row["Coefficient"]}\n')
        f.write(";\n\n")

        # param Sales_price
        f.write("param Sales_price :=\n")
        for index, row in customersSalesASPProduct_df.iterrows():
            f.write(f'"{row["Customers"]}"\t"{row["Product"]}"\t{row["ASP"]}\n')
        f.write(";\n\n")

        return customersSalesASPProduct_df

def B4C_GLPK_maxGM(model_file, dat_file, result_file):
    # Define the command to run glpsol
    command = [settings.GLPK_PATH, '--model', model_file, '--data', dat_file, '--output', result_file]

    # Execute the command
    result = subprocess.run(command, capture_output=True, text=True)

    # Read the .sol file
    with open(result_file, 'r') as sol_file:
        sol_content = sol_file.read()

    # Custom function to handle scientific notation
    def convert_to_float(value):
        try:
            return float(value.replace('D', 'E'))
        except ValueError:
            return None

    # Initialize variables to store data
    variable_data = []

    # Define patterns to identify the start of the constraints and variables sections
    constraints_pattern = re.compile(r'\s*No.\s+Row name\s+St\s+Activity\s+Lower bound\s+Upper bound\s+Marginal')
    variables_pattern = re.compile(r'\s*No.\s+Column name\s+St\s+Activity\s+Lower bound\s+Upper bound\s+Marginal')

    # Identify the start of the variables section
    variables_start = variables_pattern.search(sol_content)
    if variables_start:
        sol_content = sol_content[variables_start.end():]

    # Define a pattern to extract data for variables
    pattern = re.compile(r'(\w+)?\[(.+?)\]\s+(\w+)\s+([\d\.eE+-]+)?\s+([\d\.eE+-]+)?\s+([\d\.eE+-]+)?\s+([\d\.eE+-]+)?(?!\s*\d)')

    # Search for variable data in the .sol file
    matches = pattern.findall(sol_content)
    for match in matches:
        variable_name = match[0]
        key = match[1]
        status = match[2]

        activity = convert_to_float(match[3])
        lower_bound = convert_to_float(match[4])
        upper_bound = convert_to_float(match[5])
        marginal = convert_to_float(match[6])

        index_parts = key.split(',')
        index1 = index_parts[0].strip() if len(index_parts) >= 1 else ''
        index2 = index_parts[1].strip() if len(index_parts) >= 2 else ''
        index3 = index_parts[2].strip() if len(index_parts) >= 3 else ''
        index4 = index_parts[3].strip() if len(index_parts) >= 4 else ''
        index5 = index_parts[4].strip() if len(index_parts) >= 5 else ''

        variable_data.append({
            'Variable': variable_name,
            'index1': index1,
            'index2': index2,
            'index3': index3,
            'index4': index4,
            'index5': index5,
            'Value': activity if activity is not None else 0,
        })

    # Convert the data to a Pandas DataFrame for further analysis or export
    B4C_dat_maxGM_results_df = pd.DataFrame(variable_data)
    B4C_dat_maxGM_results_df = B4C_dat_maxGM_results_df.drop(columns=['index3', 'index3', 'index4', 'index5'])
    return B4C_dat_maxGM_results_df

def B4C_Create_maxGM_Base_Table(quarry, B4C_dat_maxGM_results_df, customersSalesASPProduct_df):
    # Retrieve dataframes
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')

    stock_Variation_df = B4C_dat_maxGM_results_df[B4C_dat_maxGM_results_df['Variable'] == 'Stock_variation'].copy()
    stock_Variation_df = stock_Variation_df.drop(columns=['Variable', 'index2'])
    stock_Variation_df = stock_Variation_df.rename(columns={'index1': 'Product', 'Value': 'Min_Stock'})
    stock_Variation_df['Product'] = stock_Variation_df['Product'].str.replace("'","")
    stock_Variation_df['Min_Stock'] = stock_Variation_df['Min_Stock'].astype(int)

    sales_df = B4C_dat_maxGM_results_df[B4C_dat_maxGM_results_df['Variable'] == 'Sales'].copy()
    sales_df = sales_df.drop(columns=['Variable'])
    sales_df = sales_df.rename(columns={'index1': 'Customers', 'index2': 'Product', 'Value': 'Sales_Volume'})
    sales_df['Customers'] = sales_df['Customers'].str.replace("'","")
    sales_df['Product'] = sales_df['Product'].str.replace("'","")
    sales_df['Sales_Volume'] = sales_df['Sales_Volume'].astype(float).round(2)

    # Get to proper ASP of Customers
    customersSalesASPProduct_df = customersSalesASPProduct_df.drop(columns=['Sales_Volume', 'TO'])
    sales_df = pd.merge(sales_df, customersSalesASPProduct_df, on=['Product', 'Customers'], how='left')
    sales_df['Turnover'] = sales_df['ASP'] * sales_df['Sales_Volume']

    # Retrieve Variable_Cost_per_t
    variable_cost_per_t_df = P4PFullSummary_df.copy()
    variable_cost_per_t_df['Variable_Cost_per_t'] = variable_cost_per_t_df['ASP'] - variable_cost_per_t_df['Agg_levy'] - variable_cost_per_t_df['GM']
    variable_cost_per_t_df = variable_cost_per_t_df.drop(
        columns=['Product_Cluster', 'Production_Volume', 'Sales_Volume', 'ASP', 'Agg_levy', 'GM', 'TM'])

    # Prepare maxGM_Base_Table_df_Downaload
    maxGM_Base_Table_df_Downaload = sales_df.copy()
    maxGM_Base_Table_df_Downaload = pd.merge(maxGM_Base_Table_df_Downaload, variable_cost_per_t_df, on='Product', how='left')
    maxGM_Base_Table_df_Downaload['Gross_Margin_per_t'] = maxGM_Base_Table_df_Downaload['ASP'] - maxGM_Base_Table_df_Downaload['Variable_Cost_per_t']
    maxGM_Base_Table_df_Downaload = maxGM_Base_Table_df_Downaload[maxGM_Base_Table_df_Downaload['Sales_Volume'] != 0]

    # Build maxGM_Base_Table_df
    maxGM_Base_Table_df = sales_df.copy()
    maxGM_Base_Table_df = maxGM_Base_Table_df.drop(columns=['Customers', 'ASP'])
    maxGM_Base_Table_df = maxGM_Base_Table_df.groupby(['Product']).agg(
        {'Sales_Volume': 'sum', 'Turnover': 'sum'}).reset_index()

    maxGM_Base_Table_df['ASP'] = maxGM_Base_Table_df['Turnover'] / maxGM_Base_Table_df['Sales_Volume']
    maxGM_Base_Table_df = pd.merge(maxGM_Base_Table_df, variable_cost_per_t_df, on='Product', how='left')
    maxGM_Base_Table_df = pd.merge(maxGM_Base_Table_df, stock_Variation_df, on='Product', how='left')
    maxGM_Base_Table_df['Production_Volume'] = maxGM_Base_Table_df['Sales_Volume'] + maxGM_Base_Table_df['Min_Stock']
    maxGM_Base_Table_df['Variable_Cost'] = maxGM_Base_Table_df['Production_Volume'] * maxGM_Base_Table_df['Variable_Cost_per_t']
    maxGM_Base_Table_df['Gross_Margin'] = maxGM_Base_Table_df['Turnover'] - maxGM_Base_Table_df['Variable_Cost']
    maxGM_Base_Table_df = maxGM_Base_Table_df.sort_values(by='Sales_Volume', ascending=False)

    # Calculate the sum of each column
    sum_row_maxGM_Base_Table = maxGM_Base_Table_df.sum(axis=0)
    sum_row_maxGM_Base_Table = sum_row_maxGM_Base_Table.drop(['Product'])
    sum_row_maxGM_Base_Table_Delta = sum_row_maxGM_Base_Table.copy()

    # Format data
    maxGM_Base_Table_df['Sales_Volume'] = maxGM_Base_Table_df['Sales_Volume'].apply(format_with_commas)
    maxGM_Base_Table_df['Min_Stock'] = maxGM_Base_Table_df['Min_Stock'].apply(format_with_commas)
    maxGM_Base_Table_df['Turnover'] = maxGM_Base_Table_df['Turnover'].apply(format_with_commas)
    maxGM_Base_Table_df['Variable_Cost'] = maxGM_Base_Table_df['Variable_Cost'].apply(format_with_commas)
    maxGM_Base_Table_df['Gross_Margin'] = maxGM_Base_Table_df['Gross_Margin'].apply(format_with_commas)
    maxGM_Base_Table_df['ASP'] = maxGM_Base_Table_df['ASP'].round(2)
    maxGM_Base_Table_df['Variable_Cost_per_t'] = maxGM_Base_Table_df['Variable_Cost_per_t'].round(2)

    sum_row_maxGM_Base_Table = sum_row_maxGM_Base_Table.apply(format_with_commas)

    return maxGM_Base_Table_df, sum_row_maxGM_Base_Table, maxGM_Base_Table_df_Downaload, sum_row_maxGM_Base_Table_Delta


def B4C_Create_modes_Hours(B4C_results_df, quarry):
    # Retrieve dataframe
    capacityModes_df_json = quarry.capacityModes_df_json
    capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
    capacityModes_df['Coefficient'] = capacityModes_df['Coefficient'] / 100

    modes_Operating_Hours_df = B4C_results_df[B4C_results_df['Variable'] == 'Operating_hours'].copy()
    modes_Operating_Hours_df = modes_Operating_Hours_df.rename(columns={'index1': 'Mode', 'Value': 'Operating_Hours'})
    modes_Operating_Hours_df = modes_Operating_Hours_df.drop(columns=['Variable'])
    modes_Opening_Hours_df = B4C_results_df[B4C_results_df['Variable'] == 'Opening_hours']
    modes_Opening_Hours_df = modes_Opening_Hours_df.rename(columns={'index1': 'Mode', 'Value': 'Opening_Hours'})
    modes_Opening_Hours_df = modes_Opening_Hours_df.drop(columns=['Variable'])
    modes_Hours_df = pd.merge(modes_Operating_Hours_df, modes_Opening_Hours_df, on='Mode', how='left')
    modes_Hours_df['Modes'] = modes_Hours_df['Mode'].str.replace("'","")
    modes_Hours_df = modes_Hours_df.drop(columns=['Mode'])
    modes_Hours_df['Operating_Hours'] = modes_Hours_df['Operating_Hours'].astype(float).round(1)
    modes_Hours_df['Opening_Hours'] = modes_Hours_df['Opening_Hours'].astype(float).round(1)
    modes_Hours_df = pd.merge(modes_Hours_df, capacityModes_df, on='Modes', how='left')

    # Calculate the sum of each column
    sum_row_modes_Hours_df = modes_Hours_df.sum(axis=0)
    sum_row_modes_Hours_df = sum_row_modes_Hours_df.drop(['Modes'])
    sum_row_modes_Hours_df['Operating_Hours'] = sum_row_modes_Hours_df['Operating_Hours'].round(1)
    sum_row_modes_Hours_df['Opening_Hours'] = sum_row_modes_Hours_df['Opening_Hours'].round(1)

    return modes_Hours_df, sum_row_modes_Hours_df

def B4C_Create_minVC_maxGM_difference_df(minVC_Base_Table_df_Downaload, maxGM_Base_Table_df_Downaload):
    minVC_Base_Table_df_Downaload = minVC_Base_Table_df_Downaload.rename(columns={
        'Sales_Volume': 'Sales_Volume_minVC',
        'ASP': 'ASP_minVC',
        'Turnover': 'Turnover_minVC',
        'Variable_Cost_per_t': 'Variable_Cost_per_t_minVC',
        'Gross_Margin_per_t': 'Gross_Margin_per_t_minVC',
    })

    maxGM_Base_Table_df_Downaload = maxGM_Base_Table_df_Downaload.rename(columns={
        'Sales_Volume': 'Sales_Volume_maxGM',
        'ASP': 'ASP_maxGM',
        'Turnover': 'Turnover_maxGM',
        'Variable_Cost_per_t': 'Variable_Cost_per_t_maxGM',
        'Gross_Margin_per_t': 'Gross_Margin_per_t_maxGM',
    })

    # Merge dataframes
    minVC_maxGM_difference_df = pd.merge(minVC_Base_Table_df_Downaload, maxGM_Base_Table_df_Downaload, on=['Customers', 'Product'], how='left')

    # Fill missing values with 0
    minVC_maxGM_difference_df = minVC_maxGM_difference_df.fillna(0)

    # Clean minVC_maxGM_difference_df
    minVC_maxGM_difference_df = minVC_maxGM_difference_df.drop(columns=['Turnover_minVC', 'ASP_maxGM', 'Turnover_maxGM', 'Variable_Cost_per_t_maxGM', 'Gross_Margin_per_t_maxGM'])
    minVC_maxGM_difference_df = minVC_maxGM_difference_df.rename(columns={
        'ASP_minVC': 'ASP',
        'Variable_Cost_per_t_minVC': 'Variable_Cost_per_t',
        'Gross_Margin_per_t_minVC': 'Gross_Margin_per_t',
    })

    # Isolate rows that show difference in Sales_Volume
    minVC_maxGM_difference_df['Sales_Volume_Delta'] = minVC_maxGM_difference_df['Sales_Volume_maxGM'] - minVC_maxGM_difference_df['Sales_Volume_minVC']
    minVC_maxGM_difference_df = minVC_maxGM_difference_df[minVC_maxGM_difference_df['Sales_Volume_Delta'] < - 0.001]
    minVC_maxGM_difference_df['Gross_Margin_Delta'] = minVC_maxGM_difference_df['Sales_Volume_Delta'] * minVC_maxGM_difference_df['Gross_Margin_per_t']
    minVC_maxGM_difference_df = minVC_maxGM_difference_df.sort_values(by='Sales_Volume_Delta')
    minVC_maxGM_difference_df['ASP'] = minVC_maxGM_difference_df['ASP'].round(2)
    minVC_maxGM_difference_df['Variable_Cost_per_t'] = minVC_maxGM_difference_df['Variable_Cost_per_t'].round(2)
    minVC_maxGM_difference_df['Gross_Margin_per_t'] = minVC_maxGM_difference_df['Gross_Margin_per_t'].round(2)
    minVC_maxGM_difference_df_Download = minVC_maxGM_difference_df.copy()

    # Format minVC_maxGM_difference_df
    minVC_maxGM_difference_df['Sales_Volume_minVC'] = minVC_maxGM_difference_df['Sales_Volume_minVC'].apply(format_with_commas)
    minVC_maxGM_difference_df['Sales_Volume_maxGM'] = minVC_maxGM_difference_df['Sales_Volume_maxGM'].apply(format_with_commas)
    minVC_maxGM_difference_df['Sales_Volume_Delta'] = minVC_maxGM_difference_df['Sales_Volume_Delta'].apply(format_with_commas)
    minVC_maxGM_difference_df['Gross_Margin_Delta'] = minVC_maxGM_difference_df['Gross_Margin_Delta'].apply(format_with_commas)

    return minVC_maxGM_difference_df, minVC_maxGM_difference_df_Download

def B4C_Summary_Delta(sum_row_minVC_Base_Table_Delta, sum_row_maxGM_Base_Table_Delta, minVC_sum_row_modes_Hours_df, maxGM_sum_row_modes_Hours_df):
    # Create a new sum_row element that sums both
    delta_row_minVC_maxGM = {}
    for key in sum_row_minVC_Base_Table_Delta.index.union(sum_row_maxGM_Base_Table_Delta.index):
        delta_row_minVC_maxGM[key] = sum_row_maxGM_Base_Table_Delta.get(key, 0) - sum_row_minVC_Base_Table_Delta.get(key, 0)
        delta_row_minVC_maxGM[key] = format_with_commas(delta_row_minVC_maxGM[key])

    sum_operating_hours = maxGM_sum_row_modes_Hours_df.get("Operating_Hours", 0) - minVC_sum_row_modes_Hours_df.get("Operating_Hours", 0)
    sum_opening_hours = maxGM_sum_row_modes_Hours_df.get("Opening_Hours", 0) - minVC_sum_row_modes_Hours_df.get("Opening_Hours", 0)

    sum_operating_hours = (sum_operating_hours).round(1)
    sum_opening_hours = (sum_opening_hours).round(1)

    return delta_row_minVC_maxGM, sum_operating_hours, sum_opening_hours