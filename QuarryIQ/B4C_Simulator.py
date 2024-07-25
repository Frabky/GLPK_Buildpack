import pandas as pd
from io import StringIO
import subprocess
import re
import os
from django.conf import settings

def format_with_commas(num):
    return "{:,.0f}".format(num)


def B4C_Get_prodSalesASP(quarry):
    # Retrieve dataframes
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
    salesSimulation_df_json = quarry.salesSimulation_df_json
    salesSimulation_df = pd.read_json(StringIO(salesSimulation_df_json), orient='records')

    # Build B4CSimulation_df
    B4CSimulation_df = P4PFullSummary_df.copy()
    B4CSimulation_df = B4CSimulation_df.drop(columns=['Product_Cluster', 'Production_Volume', 'Agg_levy', 'GM', 'TM']).copy()

    # Check if salesSimulation_df is not empty, if it is empty, B4CSimulation_df remains unchanged
    if not salesSimulation_df.empty:
        B4CSimulation_df = pd.merge(B4CSimulation_df, salesSimulation_df, on='Product', how='left')
        B4CSimulation_df['Sales_Volume_Sim'] = B4CSimulation_df['Sales_Volume_Sim'].replace({0: ''}, regex=True)
        B4CSimulation_df['ASP_Sim'] = B4CSimulation_df['ASP_Sim'].replace({0: ''}, regex=True)
        B4CSimulation_df['Opening_Stock_Sim'] = B4CSimulation_df['Opening_Stock_Sim'].replace({0: ''}, regex=True)
        B4CSimulation_df['Closing_Stock_Sim'] = B4CSimulation_df['Closing_Stock_Sim'].replace({0: ''}, regex=True)

    # Format
    B4CSimulation_df['Sales_Volume'] = B4CSimulation_df['Sales_Volume'].apply(format_with_commas)
    B4CSimulation_df['ASP'] = B4CSimulation_df['ASP'].round(2)

    return B4CSimulation_df


def B4C_dat_minVC_Simulation(quarry, salesSimulation_df, dat_file_path):
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
        for index, row in salesSimulation_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Sales_Volume_Sim"]}\n')
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

        # param Opening_stock
        f.write("param Opening_stock :=\n")
        for index, row in salesSimulation_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Opening_Stock_Sim"]}\n')
        f.write(";\n\n")

        # param Closing_stock_demand
        f.write("param Closing_stock_demand :=\n")
        for index, row in salesSimulation_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Closing_Stock_Sim"]}\n')
        f.write(";\n\n")


def B4C_GLPK_minVC_Simulation(model_file, dat_file, result_file):
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
    B4C_dat_minVC_Simulation_results_df = pd.DataFrame(variable_data)
    B4C_dat_minVC_Simulation_results_df = B4C_dat_minVC_Simulation_results_df.drop(columns=['index2', 'index3', 'index3', 'index4', 'index5'])
    return B4C_dat_minVC_Simulation_results_df

def B4C_Create_minVC_Simulation_Table(quarry, B4C_dat_minVC_Simulation_results_df, salesSimulation_df):
    # Retrieve dataframes
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')

    # Retrieve Sales_Volume_Sim & ASP_Sim
    salesSimulation_df = salesSimulation_df.drop(columns=['Opening_Stock_Sim', 'Closing_Stock_Sim'])

    # Retrieve Variable_Cost_per_t
    variable_cost_per_t_df = P4PFullSummary_df.copy()
    variable_cost_per_t_df['Variable_Cost_per_t'] = variable_cost_per_t_df['ASP'] - variable_cost_per_t_df['Agg_levy'] - variable_cost_per_t_df['GM']
    variable_cost_per_t_df = variable_cost_per_t_df.drop(
        columns=['Product_Cluster', 'Production_Volume', 'Sales_Volume', 'ASP', 'Agg_levy', 'GM', 'TM'])

    # Calculate stock_Variation_df
    stock_Variation_df = B4C_dat_minVC_Simulation_results_df[B4C_dat_minVC_Simulation_results_df['Variable'] == 'Closing_stock'].copy()
    stock_Variation_df = stock_Variation_df.drop(columns=['Variable'])
    stock_Variation_df = stock_Variation_df.rename(columns={'index1': 'Product', 'Value': 'Min_Closing_Stock'})
    stock_Variation_df['Product'] = stock_Variation_df['Product'].str.replace("'", "")
    stock_Variation_df['Min_Closing_Stock'] = stock_Variation_df['Min_Closing_Stock'].astype(int)

    # Calculate Variable_cost_df
    Variable_cost_df = B4C_dat_minVC_Simulation_results_df[B4C_dat_minVC_Simulation_results_df['Variable'] == 'Variable_cost_calc'].copy()
    Variable_cost_df = Variable_cost_df.drop(columns=['Variable'])
    Variable_cost_df = Variable_cost_df.rename(columns={'index1': 'Product', 'Value': 'Variable_Cost'})
    Variable_cost_df['Product'] = Variable_cost_df['Product'].str.replace("'", "")
    Variable_cost_df['Variable_Cost'] = Variable_cost_df['Variable_Cost'].astype(int)

    # Create minVC_Simulation_Table_df
    minVC_Simulation_Table_df = pd.merge(salesSimulation_df, variable_cost_per_t_df, on='Product', how='left')
    minVC_Simulation_Table_df = pd.merge(minVC_Simulation_Table_df, stock_Variation_df, on='Product', how='left')
    minVC_Simulation_Table_df = pd.merge(minVC_Simulation_Table_df, Variable_cost_df, on='Product', how='left')
    minVC_Simulation_Table_df['Turnover'] = minVC_Simulation_Table_df['Sales_Volume_Sim'] * minVC_Simulation_Table_df['ASP_Sim']
    minVC_Simulation_Table_df['Gross_Margin'] = minVC_Simulation_Table_df['Turnover'] - minVC_Simulation_Table_df['Variable_Cost']

    return minVC_Simulation_Table_df

def B4C_Create_modes_Hours_minVC(B4C_dat_minVC_Simulation_results_df, quarry):
    # Retrieve dataframe
    capacityModes_df_json = quarry.capacityModes_df_json
    capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
    capacityModes_df['Coefficient'] = capacityModes_df['Coefficient'] / 100

    modes_Operating_Hours_df = B4C_dat_minVC_Simulation_results_df[B4C_dat_minVC_Simulation_results_df['Variable'] == 'Operating_hours'].copy()
    modes_Operating_Hours_df = modes_Operating_Hours_df.rename(columns={'index1': 'Mode', 'Value': 'Operating_Hours'})
    modes_Operating_Hours_df = modes_Operating_Hours_df.drop(columns=['Variable'])
    modes_Opening_Hours_df = B4C_dat_minVC_Simulation_results_df[B4C_dat_minVC_Simulation_results_df['Variable'] == 'Opening_hours']
    modes_Opening_Hours_df = modes_Opening_Hours_df.rename(columns={'index1': 'Mode', 'Value': 'Opening_Hours'})
    modes_Opening_Hours_df = modes_Opening_Hours_df.drop(columns=['Variable'])
    modes_Hours_minVC_Simulation_df = pd.merge(modes_Operating_Hours_df, modes_Opening_Hours_df, on='Mode', how='left')
    modes_Hours_minVC_Simulation_df['Modes'] = modes_Hours_minVC_Simulation_df['Mode'].str.replace("'","")
    modes_Hours_minVC_Simulation_df = modes_Hours_minVC_Simulation_df.drop(columns=['Mode'])
    modes_Hours_minVC_Simulation_df['Operating_Hours'] = modes_Hours_minVC_Simulation_df['Operating_Hours'].astype(float).round(1)
    modes_Hours_minVC_Simulation_df['Opening_Hours'] = modes_Hours_minVC_Simulation_df['Opening_Hours'].astype(float).round(1)
    modes_Hours_minVC_Simulation_df = pd.merge(modes_Hours_minVC_Simulation_df, capacityModes_df, on='Modes', how='left')

    return modes_Hours_minVC_Simulation_df

def B4C_dat_maxGM_Simulation(quarry, salesSimulation_df, dat_file_path):
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
        for index, row in salesSimulation_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Sales_Volume_Sim"]}\n')
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

        # param Opening_stock
        f.write("param Opening_stock :=\n")
        for index, row in salesSimulation_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Opening_Stock_Sim"]}\n')
        f.write(";\n\n")

        # param Closing_stock_demand
        f.write("param Closing_stock_demand :=\n")
        for index, row in salesSimulation_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Closing_Stock_Sim"]}\n')
        f.write(";\n\n")

        # param Sales_price
        f.write("param Sales_price :=\n")
        for index, row in salesSimulation_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["ASP_Sim"]}\n')
        f.write(";\n\n")


def B4C_GLPK_maxGM_Simulation(model_file, dat_file, result_file):
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

    if os.path.exists(dat_file):
        # Delete the file
        os.remove(dat_file)
    if os.path.exists(result_file):
        # Delete the file
        os.remove(result_file)

    # Convert the data to a Pandas DataFrame for further analysis or export
    B4C_dat_maxGM_Simulation_results_df = pd.DataFrame(variable_data)
    B4C_dat_maxGM_Simulation_results_df = B4C_dat_maxGM_Simulation_results_df.drop(columns=['index2', 'index3', 'index3', 'index4', 'index5'])
    return B4C_dat_maxGM_Simulation_results_df


def B4C_Create_maxGM_Simulation_Table(quarry, B4C_dat_maxGM_Simulation_results_df, salesSimulation_df):
    # Retrieve dataframes
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')

    # Retrieve ASP_Sim
    salesSimulation_df = salesSimulation_df.drop(columns=['Sales_Volume_Sim', 'Opening_Stock_Sim', 'Closing_Stock_Sim'])

    # Retrieve Variable_Cost_per_t
    variable_cost_per_t_df = P4PFullSummary_df.copy()
    variable_cost_per_t_df['Variable_Cost_per_t'] = variable_cost_per_t_df['ASP'] - variable_cost_per_t_df['Agg_levy'] - variable_cost_per_t_df['GM']
    variable_cost_per_t_df = variable_cost_per_t_df.drop(
        columns=['Product_Cluster', 'Production_Volume', 'Sales_Volume', 'ASP', 'Agg_levy', 'GM', 'TM'])

    # Calculate stock_Variation_df
    stock_Variation_df = B4C_dat_maxGM_Simulation_results_df[B4C_dat_maxGM_Simulation_results_df['Variable'] == 'Closing_stock'].copy()
    stock_Variation_df = stock_Variation_df.drop(columns=['Variable'])
    stock_Variation_df = stock_Variation_df.rename(columns={'index1': 'Product', 'Value': 'Min_Closing_Stock'})
    stock_Variation_df['Product'] = stock_Variation_df['Product'].str.replace("'", "")
    stock_Variation_df['Min_Closing_Stock'] = stock_Variation_df['Min_Closing_Stock'].astype(int)

    # Calculate sales_df
    sales_df = B4C_dat_maxGM_Simulation_results_df[B4C_dat_maxGM_Simulation_results_df['Variable'] == 'Sales'].copy()
    sales_df = sales_df.drop(columns=['Variable'])
    sales_df = sales_df.rename(columns={'index1': 'Product', 'Value': 'Sales_Volume_Sim'})
    sales_df['Product'] = sales_df['Product'].str.replace("'", "")
    sales_df['Sales_Volume_Sim'] = sales_df['Sales_Volume_Sim'].astype(int)

    # Calculate Variable_cost_df
    Variable_cost_df = B4C_dat_maxGM_Simulation_results_df[B4C_dat_maxGM_Simulation_results_df['Variable'] == 'Variable_cost_calc'].copy()
    Variable_cost_df = Variable_cost_df.drop(columns=['Variable'])
    Variable_cost_df = Variable_cost_df.rename(columns={'index1': 'Product', 'Value': 'Variable_Cost'})
    Variable_cost_df['Product'] = Variable_cost_df['Product'].str.replace("'", "")
    Variable_cost_df['Variable_Cost'] = Variable_cost_df['Variable_Cost'].astype(int)

    # Create maxGM_Simulation_Table_df
    maxGM_Simulation_Table_df = pd.merge(salesSimulation_df, variable_cost_per_t_df, on='Product', how='left')
    maxGM_Simulation_Table_df = pd.merge(maxGM_Simulation_Table_df, stock_Variation_df, on='Product', how='left')
    maxGM_Simulation_Table_df = pd.merge(maxGM_Simulation_Table_df, sales_df, on='Product', how='left')
    maxGM_Simulation_Table_df = pd.merge(maxGM_Simulation_Table_df, Variable_cost_df, on='Product', how='left')
    maxGM_Simulation_Table_df['Turnover'] = maxGM_Simulation_Table_df['Sales_Volume_Sim'] * maxGM_Simulation_Table_df['ASP_Sim']
    maxGM_Simulation_Table_df['Gross_Margin'] = maxGM_Simulation_Table_df['Turnover'] - maxGM_Simulation_Table_df['Variable_Cost']

    return maxGM_Simulation_Table_df


def B4C_Create_modes_Hours_maxGM(B4C_dat_maxGM_Simulation_results_df, quarry):
    # Retrieve dataframe
    capacityModes_df_json = quarry.capacityModes_df_json
    capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
    capacityModes_df['Coefficient'] = capacityModes_df['Coefficient'] / 100

    modes_Operating_Hours_df = B4C_dat_maxGM_Simulation_results_df[B4C_dat_maxGM_Simulation_results_df['Variable'] == 'Operating_hours'].copy()
    modes_Operating_Hours_df = modes_Operating_Hours_df.rename(columns={'index1': 'Mode', 'Value': 'Operating_Hours'})
    modes_Operating_Hours_df = modes_Operating_Hours_df.drop(columns=['Variable'])
    modes_Opening_Hours_df = B4C_dat_maxGM_Simulation_results_df[B4C_dat_maxGM_Simulation_results_df['Variable'] == 'Opening_hours']
    modes_Opening_Hours_df = modes_Opening_Hours_df.rename(columns={'index1': 'Mode', 'Value': 'Opening_Hours'})
    modes_Opening_Hours_df = modes_Opening_Hours_df.drop(columns=['Variable'])
    modes_Hours_maxGM_Simulation_df = pd.merge(modes_Operating_Hours_df, modes_Opening_Hours_df, on='Mode', how='left')
    modes_Hours_maxGM_Simulation_df['Modes'] = modes_Hours_maxGM_Simulation_df['Mode'].str.replace("'","")
    modes_Hours_maxGM_Simulation_df = modes_Hours_maxGM_Simulation_df.drop(columns=['Mode'])
    modes_Hours_maxGM_Simulation_df['Operating_Hours'] = modes_Hours_maxGM_Simulation_df['Operating_Hours'].astype(float).round(1)
    modes_Hours_maxGM_Simulation_df['Opening_Hours'] = modes_Hours_maxGM_Simulation_df['Opening_Hours'].astype(float).round(1)
    modes_Hours_maxGM_Simulation_df = pd.merge(modes_Hours_maxGM_Simulation_df, capacityModes_df, on='Modes', how='left')

    return modes_Hours_maxGM_Simulation_df

def B4C_Summary_Delta(sum_row_minVC_Simulation_Table, sum_row_maxGM_Simulation_Table, sum_row_minVC_modes_Hours, sum_row_maxGM_modes_Hours):
    # Create a new sum_row element that sums both
    delta_row_minVC_maxGM = {}
    for key in sum_row_minVC_Simulation_Table.index.union(sum_row_maxGM_Simulation_Table.index):
        delta_row_minVC_maxGM[key] = sum_row_maxGM_Simulation_Table.get(key, 0) - sum_row_minVC_Simulation_Table.get(key, 0)
        delta_row_minVC_maxGM[key] = format_with_commas(delta_row_minVC_maxGM[key])

    sum_operating_hours = sum_row_maxGM_modes_Hours.get("Operating_Hours", 0) - sum_row_minVC_modes_Hours.get("Operating_Hours", 0)
    sum_opening_hours = sum_row_maxGM_modes_Hours.get("Opening_Hours", 0) - sum_row_minVC_modes_Hours.get("Opening_Hours", 0)

    sum_operating_hours = (sum_operating_hours).round(1)
    sum_opening_hours = (sum_opening_hours).round(1)

    return delta_row_minVC_maxGM, sum_operating_hours, sum_opening_hours


def Retrieve_dataframes(quarry):
    minVC_Simulation_Table_df_json = quarry.minVC_Simulation_Table_df_json
    minVC_Simulation_Table_df = pd.read_json(StringIO(minVC_Simulation_Table_df_json), orient='records')

    modes_Hours_minVC_Simulation_df_json = quarry.modes_Hours_minVC_Simulation_df_json
    modes_Hours_minVC_Simulation_df = pd.read_json(StringIO(modes_Hours_minVC_Simulation_df_json), orient='records')

    maxGM_Simulation_Table_df_json = quarry.maxGM_Simulation_Table_df_json
    maxGM_Simulation_Table_df = pd.read_json(StringIO(maxGM_Simulation_Table_df_json), orient='records')

    modes_Hours_maxGM_Simulation_df_json = quarry.modes_Hours_maxGM_Simulation_df_json
    modes_Hours_maxGM_Simulation_df = pd.read_json(StringIO(modes_Hours_maxGM_Simulation_df_json), orient='records')


    return minVC_Simulation_Table_df, modes_Hours_minVC_Simulation_df, maxGM_Simulation_Table_df, modes_Hours_maxGM_Simulation_df