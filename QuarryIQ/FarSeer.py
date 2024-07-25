import pandas as pd
from io import StringIO
import subprocess
import re
import os
import plotly.graph_objects as go
import plotly.io as pio
from django.conf import settings
from .utils import get_mod_file_content
import tempfile


# Dictionary mapping month names to numbers
month_to_number = {
    'January': 1,
    'February': 2,
    'March': 3,
    'April': 4,
    'May': 5,
    'June': 6,
    'July': 7,
    'August': 8,
    'September': 9,
    'October': 10,
    'November': 11,
    'December': 12
}

def format_with_commas(num):
    return "{:,.0f}".format(num)

def FarSeer_Create_SalesPerMonth_df(project_data, quarry):
    # Retrieve modified_transactions_day_df
    modified_transactions_day_df_json = project_data.modified_transactions_day_df_json
    modified_transactions_day_df = pd.read_json(StringIO(modified_transactions_day_df_json), orient='records')

    # Retrieve startingMonth and finalMonth
    startingMonth = quarry.startingMonth
    finalMonth = quarry.finalMonth
    # Convert startingMonth and finalMonth to numbers
    startingMonth = month_to_number.get(startingMonth)
    finalMonth = month_to_number.get(finalMonth)

    # Extract the quarry related data and convert day column to readable data
    SalesPerMonth_df = modified_transactions_day_df.copy()
    SalesPerMonth_df = SalesPerMonth_df[SalesPerMonth_df['Quarries'] == quarry.name]
    SalesPerMonth_df['Day'] = pd.to_datetime(SalesPerMonth_df['Day'], unit='ms')
    SalesPerMonth_df['Month'] = SalesPerMonth_df['Day'].dt.month

    # Select columns
    SalesPerMonth_df = SalesPerMonth_df[['Product', 'Month', 'Sales_Volume', 'Incoterms', 'Revenue']]

    # Grouping by specified columns and aggregating
    SalesPerMonth_df = SalesPerMonth_df.groupby(['Product', 'Month', 'Incoterms']).agg({
        'Sales_Volume': 'sum',
        'Revenue': 'sum'
    }).reset_index()

    # Find unique_products & unique_incoterms
    unique_products = SalesPerMonth_df['Product'].unique()
    unique_incoterms = SalesPerMonth_df['Incoterms'].unique()

    # Generate all unique combinations of products and months
    all_combinations = pd.MultiIndex.from_product([unique_products, range(startingMonth, finalMonth + 1), unique_incoterms],
                                                  names=['Product', 'Month', 'Incoterms'])
    all_combinations_df = pd.DataFrame(index=all_combinations).reset_index()

    # Merge the filtered DataFrame with all combinations DataFrame
    SalesPerMonth_df = pd.merge(all_combinations_df, SalesPerMonth_df, on=['Product', 'Month', 'Incoterms'], how='left')

    # Fill NaN values with 0
    SalesPerMonth_df = SalesPerMonth_df.fillna(0)

    # Create new columns for Sales_Volume_DAP and ASP_DAP and for Sales_Volume_EXW and ASP_EXW
    SalesPerMonth_df['Sales_Volume_DAP'] = SalesPerMonth_df[SalesPerMonth_df['Incoterms'] == 'Delivered']['Sales_Volume']
    SalesPerMonth_df['Revenue_DAP'] = SalesPerMonth_df[SalesPerMonth_df['Incoterms'] == 'Delivered']['Revenue']
    SalesPerMonth_df['Sales_Volume_EXW'] = SalesPerMonth_df[SalesPerMonth_df['Incoterms'] == 'Ex-works']['Sales_Volume']
    SalesPerMonth_df['Revenue_EXW'] = SalesPerMonth_df[SalesPerMonth_df['Incoterms'] == 'Ex-works']['Revenue']


    # Drop columns and fill with 0
    SalesPerMonth_df = SalesPerMonth_df.drop(columns=['Incoterms', 'Revenue'])
    SalesPerMonth_df = SalesPerMonth_df.fillna(0)

    # Groupby
    SalesPerMonth_df = SalesPerMonth_df.groupby(['Product', 'Month']).agg({
        'Sales_Volume_DAP': 'sum',
        'Sales_Volume_EXW': 'sum',
        'Revenue_DAP': 'sum',
        'Revenue_EXW': 'sum'
    }).reset_index()

    # ASP per incoterm caluclation
    SalesPerMonth_df['ASP_DAP'] = SalesPerMonth_df['Revenue_DAP'] / SalesPerMonth_df['Sales_Volume_DAP']
    SalesPerMonth_df['ASP_EXW'] = SalesPerMonth_df['Revenue_EXW'] / SalesPerMonth_df['Sales_Volume_EXW']
    SalesPerMonth_df = SalesPerMonth_df.fillna(0)

    # Sales_Volume_Total Calculation
    SalesPerMonth_df['Sales_Volume_Total'] = SalesPerMonth_df['Sales_Volume_DAP'] + SalesPerMonth_df['Sales_Volume_EXW']
    SalesPerMonth_df = SalesPerMonth_df.sort_values(by=['Month', 'Sales_Volume_Total'], ascending=[True, False])

    # Format
    SalesPerMonth_df['Sales_Volume_DAP'] = SalesPerMonth_df['Sales_Volume_DAP'].apply(format_with_commas)
    SalesPerMonth_df['ASP_DAP'] = SalesPerMonth_df['ASP_DAP'].round(2)
    SalesPerMonth_df['Sales_Volume_EXW'] = SalesPerMonth_df['Sales_Volume_EXW'].apply(format_with_commas)
    SalesPerMonth_df['ASP_EXW'] = SalesPerMonth_df['ASP_EXW'].round(2)

    # Add a new column to the DataFrame with month names
    SalesPerMonth_df['Month_Name'] = SalesPerMonth_df['Month'].map({v: k for k, v in month_to_number.items()})

    return SalesPerMonth_df


def FarSeer_maxOpeningHours_Table(SalesPerMonth_df, maxOpeningHours_df, quarry):
    # Get unique_months in SalesPerMonth_df and create dataframe
    unique_months = SalesPerMonth_df['Month_Name'].unique()
    unique_months_df = pd.DataFrame({'Month_Name': unique_months})

    if not maxOpeningHours_df.empty:
        # Merge dataframes with all combinations on Month_Name
        maxOpeningHours_Table = pd.merge(unique_months_df, maxOpeningHours_df, on=['Month_Name'], how='left')

        maxOpeningHours_Table = maxOpeningHours_Table[['Month_Name', 'Max_Opening_Hours']]

        # Fill NaN values with 0 before converting to integer
        maxOpeningHours_Table['Max_Opening_Hours'] = maxOpeningHours_Table['Max_Opening_Hours'].fillna(0)


        # Convert 'Max_Opening_Hours' column to integer type
        maxOpeningHours_Table['Max_Opening_Hours'] = maxOpeningHours_Table['Max_Opening_Hours'].astype(int)
    else:
        maxOpeningHours_Table = pd.DataFrame(columns=['Month_Name', 'Max_Opening_Hours'])  # Create an empty DataFrame
        maxOpeningHours_Table = pd.merge(unique_months_df, maxOpeningHours_Table, on=['Month_Name'], how='outer')

        # Fill NaN values with 0 before converting to integer
        maxOpeningHours_Table['Max_Opening_Hours'] = maxOpeningHours_Table['Max_Opening_Hours'].fillna(0)


    return maxOpeningHours_Table


def FarSeer_salesForecast_Table(SalesPerMonth_df, salesForecast_df):
    # Get unique_months in SalesPerMonth_df and create dataframe
    unique_months = SalesPerMonth_df['Month_Name'].unique()
    unique_months_df = pd.DataFrame({'Month_Name': unique_months})

    # Get unique_products in salesForecast_df and create dataframe
    unique_products = SalesPerMonth_df['Product'].unique()
    unique_products_df = pd.DataFrame({'Product': unique_products})

    # Create dataframe being combination of unique_month_df and unique_products_df
    salesForecast_Table = pd.merge(unique_months_df.assign(key=0), unique_products_df.assign(key=0), on='key').drop(columns='key')

    if not salesForecast_df.empty:
        # Merge salesForecast_Table with salesForecast_df
        salesForecast_Table = pd.merge(salesForecast_Table, salesForecast_df, on=['Product', 'Month_Name'], how='outer')

        # Merge salesForecast_Table with SalesPerMonth_df
        salesForecast_Table = pd.merge(SalesPerMonth_df, salesForecast_Table, on=['Product', 'Month_Name'], how='outer')

        # Fill NaN values with '' in the salesForecast_Table DataFrame
        salesForecast_Table = salesForecast_Table.fillna(0)


        # Replace 0's by '' & format
        salesForecast_Table['Sales_Volume_Forced'] = salesForecast_Table['Sales_Volume_Forced'].astype(float).round().astype(int)
        salesForecast_Table['Sales_Volume_Forced'] = salesForecast_Table['Sales_Volume_Forced'].replace(0, '')

        salesForecast_Table['ASP_Forced'] = salesForecast_Table['ASP_Forced'].astype(float).round(2)
        salesForecast_Table['ASP_Forced'] = salesForecast_Table['ASP_Forced'].replace(0, '')

        salesForecast_Table['Sales_Volume_Non_Essential'] = salesForecast_Table['Sales_Volume_Non_Essential'].astype(float).round().astype(int)
        salesForecast_Table['Sales_Volume_Non_Essential'] = salesForecast_Table['Sales_Volume_Non_Essential'].replace(0, '')


        salesForecast_Table['ASP_Non_Essential'] = salesForecast_Table['ASP_Non_Essential'].astype(float).round(2)
        salesForecast_Table['ASP_Non_Essential'] = salesForecast_Table['ASP_Non_Essential'].replace(0, '')


    else:
        # Merge salesForecast_Table with SalesPerMonth_df
        salesForecast_Table = pd.merge(SalesPerMonth_df, salesForecast_Table, on=['Product', 'Month_Name'], how='outer')

    return salesForecast_Table


def FarSeer_Format_salesForecast_df(salesForecast_df, maxOpeningHours_df):
    salesForecast_df['Sales_Volume_Forced'] = salesForecast_df['Sales_Volume_Forced'].replace({'': '0'}, regex=True)
    salesForecast_df['Sales_Volume_Forced'] = salesForecast_df['Sales_Volume_Forced'].astype(float).round().astype(int)
    salesForecast_df['ASP_Forced'] = salesForecast_df['ASP_Forced'].replace({'': '0'}, regex=True)
    salesForecast_df['ASP_Forced'] = salesForecast_df['ASP_Forced'].astype(float).round(2)
    salesForecast_df['Sales_Volume_Non_Essential'] = salesForecast_df['Sales_Volume_Non_Essential'].replace({'': '0'},regex=True)
    salesForecast_df['Sales_Volume_Non_Essential'] = salesForecast_df['Sales_Volume_Non_Essential'].astype(float).round().astype(int)
    salesForecast_df['ASP_Non_Essential'] = salesForecast_df['ASP_Non_Essential'].replace({'': '0'}, regex=True)
    salesForecast_df['ASP_Non_Essential'] = salesForecast_df['ASP_Non_Essential'].astype(float).round(2)
    salesForecast_df['Month'] = salesForecast_df['Month_Name'].map(month_to_number)

    maxOpeningHours_df['Max_Opening_Hours'] = maxOpeningHours_df['Max_Opening_Hours'].replace({'': '0'}, regex=True)
    maxOpeningHours_df['Max_Opening_Hours'] = maxOpeningHours_df['Max_Opening_Hours'].astype(float).round().astype(int)
    maxOpeningHours_df['Month'] = maxOpeningHours_df['Month'].map(month_to_number)
    maxOpeningHours_df['Month_Name'] = maxOpeningHours_df['Month'].map({v: k for k, v in month_to_number.items()})

    return salesForecast_df, maxOpeningHours_df


def FarSeer_Opt(quarry):

    # Optimisation meetAllSales
    # Define model
    mod_file = 'FarSeer_meetAllSales'
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Get the content of the .mod file
        mod_content = get_mod_file_content(mod_file)
        if not mod_content:
            print("Failed to retrieve and decrypt the .mod file.")
            return  # Handle the error as appropriate for your application

        # Create the .mod file in the temporary directory
        mod_file_path = os.path.join(temp_dir, mod_file)
        with open(mod_file_path, 'w') as mod_file:
            mod_file.write(mod_content)

        # .dat creation
        dat_file_path = os.path.join(temp_dir, 'FarSeer_meetAllSales.dat')
        result_file_path = os.path.join(temp_dir, 'FarSeer_meetAllSales.sol')

        # Run Optimisation
        FarSeer_dat(quarry, dat_file_path)
        FarSeer_meetAllSales_results_df, FarSeer_meetAllSales_hours_df = FarSeer_GLPK(mod_file_path, dat_file_path, result_file_path, quarry)


    # Optimisation meetForcedSales
    # Define model
    mod_file = 'FarSeer_meetForcedSales'
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Get the content of the .mod file
        mod_content = get_mod_file_content(mod_file)
        if not mod_content:
            print("Failed to retrieve and decrypt the .mod file.")
            return  # Handle the error as appropriate for your application

        # Create the .mod file in the temporary directory
        mod_file_path = os.path.join(temp_dir, mod_file)
        with open(mod_file_path, 'w') as mod_file:
            mod_file.write(mod_content)

        # .dat creation
        dat_file_path = os.path.join(temp_dir, 'FarSeer_meetForcedSales.dat')
        result_file_path = os.path.join(temp_dir, 'FarSeer_meetForcedSales.sol')

        # Run Optimisation
        FarSeer_dat(quarry, dat_file_path)
        FarSeer_meetForcedSales_results_df, FarSeer_meetForcedSales_hours_df = FarSeer_GLPK(mod_file_path, dat_file_path, result_file_path, quarry)


    # Optimisation maxGM
        # Define model
        mod_file = 'FarSeer_maxGM'
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Get the content of the .mod file
            mod_content = get_mod_file_content(mod_file)
            if not mod_content:
                print("Failed to retrieve and decrypt the .mod file.")
                return  # Handle the error as appropriate for your application

            # Create the .mod file in the temporary directory
            mod_file_path = os.path.join(temp_dir, mod_file)
            with open(mod_file_path, 'w') as mod_file:
                mod_file.write(mod_content)

            # .dat creation
            dat_file_path = os.path.join(temp_dir, 'FarSeer_maxGM.dat')
            result_file_path = os.path.join(temp_dir, 'FarSeer_maxGM.sol')

            # Run Optimisation
            FarSeer_dat(quarry, dat_file_path)
            FarSeer_maxGM_results_df, FarSeer_maxGM_hours_df = FarSeer_GLPK(mod_file_path, dat_file_path, result_file_path, quarry)


    return FarSeer_meetAllSales_results_df, FarSeer_meetAllSales_hours_df, FarSeer_meetForcedSales_results_df, FarSeer_meetForcedSales_hours_df, FarSeer_maxGM_results_df, FarSeer_maxGM_hours_df


def FarSeer_dat(quarry, dat_file):
    # Retrieve dataframes
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
    capacityModes_df_json = quarry.capacityModes_df_json
    capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
    constituentModesBalance_df_json = quarry.constituentModesBalance_df_json
    constituentModesBalance_df = pd.read_json(StringIO(constituentModesBalance_df_json), orient='records')
    productComposition_df_json = quarry.productComposition_df_json
    productComposition_df = pd.read_json(StringIO(productComposition_df_json), orient='records')
    startingMonth = quarry.startingMonth
    finalMonth = quarry.finalMonth
    salesForecast_df_json = quarry.salesForecast_df_json
    salesForecast_df = pd.read_json(StringIO(salesForecast_df_json), orient='records')
    FarSeer_stockInfo_df_json = quarry.FarSeer_stockInfo_df_json
    FarSeer_stockInfo_df = pd.read_json(StringIO(FarSeer_stockInfo_df_json), orient='records')
    maxOpeningHours_df_json = quarry.maxOpeningHours_df_json
    maxOpeningHours_df = pd.read_json(StringIO(maxOpeningHours_df_json), orient='records')


    # Get unique elements in dataframes for SET
    unique_modes = capacityModes_df['Modes'].unique()
    unique_constituents = constituentModesBalance_df['Constituent'].unique()
    unique_products = P4PFullSummary_df['Product'].unique()
    # Convert startingMonth and finalMonth to numbers
    startingMonth = month_to_number.get(startingMonth)
    finalMonth = month_to_number.get(finalMonth)
    # Get unique month for SET
    month_list = list(range(startingMonth, finalMonth + 1))

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
    with open(dat_file, "w") as f:
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

        # SET MONTHS
        f.write("set MONTHS :=\n")
        for month in month_list:
            f.write(f'{month}\n')
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

        # param Sales_volume_forced
        f.write("param Sales_Volume_Forced :=\n")
        for index, row in salesForecast_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Month"]}\t{row["Sales_Volume_Forced"]} \n')
        f.write(";\n\n")

        # param Sales_volume_non_essential
        f.write("param Sales_Volume_Non_Essential :=\n")
        for index, row in salesForecast_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Month"]}\t{row["Sales_Volume_Non_Essential"]} \n')
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

        # param ASP_forced
        f.write("param ASP_Forced :=\n")
        for index, row in salesForecast_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Month"]}\t{row["ASP_Forced"]} \n')
        f.write(";\n\n")

        # param ASP_non_essential
        f.write("param ASP_Non_Essential :=\n")
        for index, row in salesForecast_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Month"]}\t{row["ASP_Non_Essential"]} \n')
        f.write(";\n\n")

        # param Opening_stock_init
        f.write("param Stock_Starting_Month :=\n")
        for index, row in FarSeer_stockInfo_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Stock_Starting_Month"]}\n')
        f.write(";\n\n")

        # param Final_closing_stock_demand
        f.write("param Stock_Final_Month :=\n")
        for index, row in FarSeer_stockInfo_df.iterrows():
            f.write(f'"{row["Product"]}"\t{row["Stock_Final_Month"]}\n')
        f.write(";\n\n")

        # param Max_opening_hours
        f.write("param Max_Opening_Hours :=\n")
        for index, row in maxOpeningHours_df.iterrows():
            f.write(f'{row["Month"]}\t{row["Max_Opening_Hours"]}\n')
        f.write(";\n\n")


def FarSeer_GLPK(model_file, dat_file, result_file, quarry):
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
    FarSeer_raw_results_df= pd.DataFrame(variable_data)
    FarSeer_raw_results_df = FarSeer_raw_results_df.drop(columns=['index3', 'index3', 'index4', 'index5'])

    # Change Month number into Month name
    FarSeer_raw_results_df['index2'] = FarSeer_raw_results_df['index2'].astype(int)
    FarSeer_raw_results_df['index2'] = FarSeer_raw_results_df['index2'].map({v: k for k, v in month_to_number.items()})
    FarSeer_raw_results_df = FarSeer_raw_results_df.rename(columns={'index2': 'Month_Name'})


    # === Create Full Summary Table per Scenario ===

    # Retrieve salesForecast_df
    salesForecast_df_json = quarry.salesForecast_df_json
    salesForecast_df = pd.read_json(StringIO(salesForecast_df_json), orient='records')

    # Construct Triplet [Product - Month_Name - Sales_Forced] #1 // Calculated Forced Sales Volumes
    FarSeer_results_SalesForced_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Sales_Forced'].copy()
    FarSeer_results_SalesForced_df = FarSeer_results_SalesForced_df.drop(columns=['Variable'])
    FarSeer_results_SalesForced_df = FarSeer_results_SalesForced_df.rename(columns={'index1': 'Product', 'Value': 'Sales_Forced'})
    FarSeer_results_SalesForced_df['Product'] = FarSeer_results_SalesForced_df['Product'].str.replace("'","")
    FarSeer_results_SalesForced_df['Sales_Forced'] = FarSeer_results_SalesForced_df['Sales_Forced'].astype(int)

    # Construct Triplet [Product - Month_Name - ASP_Forced] #2 // Retrieve ASP for Forecast Forced Sales Volumes
    FarSeer_results_ASP_Forced_df = salesForecast_df.copy()
    FarSeer_results_ASP_Forced_df = FarSeer_results_ASP_Forced_df[['Product','Month_Name', 'ASP_Forced']]

    # Construct Triplet [Product - Month_Name - Sales_NonEssential] #3 // Calculated Non Essential Sales Volumes
    FarSeer_results_SalesNonEssential_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Sales_NonEssential'].copy()
    FarSeer_results_SalesNonEssential_df = FarSeer_results_SalesNonEssential_df.drop(columns=['Variable'])
    FarSeer_results_SalesNonEssential_df = FarSeer_results_SalesNonEssential_df.rename(columns={'index1': 'Product', 'Value': 'Sales_Non_Essential'})
    FarSeer_results_SalesNonEssential_df['Product'] = FarSeer_results_SalesNonEssential_df['Product'].str.replace("'","")
    FarSeer_results_SalesNonEssential_df['Sales_Non_Essential'] = FarSeer_results_SalesNonEssential_df['Sales_Non_Essential'].astype(int)

    # Construct Triplet [Product - Month_Name - ASP_Non_Essential] #4 // Retrieve ASP for Forecast Non Essential Sales Volumes
    FarSeer_results_ASP_NonEssential_df = salesForecast_df.copy()
    FarSeer_results_ASP_NonEssential_df = FarSeer_results_ASP_NonEssential_df[['Product', 'Month_Name', 'ASP_Non_Essential']]

    # Construct Triplet [Product - Month_Name - Opening_Stock] #5 // Calculated Opening Stock per Month
    FarSeer_results_OpeningStock_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Opening_stock'].copy()
    FarSeer_results_OpeningStock_df = FarSeer_results_OpeningStock_df.drop(columns=['Variable'])
    FarSeer_results_OpeningStock_df = FarSeer_results_OpeningStock_df.rename(columns={'index1': 'Product', 'Value': 'Opening_Stock'})
    FarSeer_results_OpeningStock_df['Product'] = FarSeer_results_OpeningStock_df['Product'].str.replace("'","")
    FarSeer_results_OpeningStock_df['Opening_Stock'] = FarSeer_results_OpeningStock_df['Opening_Stock'].astype(int)

    # Construct Triplet [Product - Month_Name - Closing_Stock] #6 // Calculated Closing Stock per Month
    FarSeer_results_ClosingStock_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Closing_stock'].copy()
    FarSeer_results_ClosingStock_df = FarSeer_results_ClosingStock_df.drop(columns=['Variable'])
    FarSeer_results_ClosingStock_df = FarSeer_results_ClosingStock_df.rename(columns={'index1': 'Product', 'Value': 'Closing_Stock'})
    FarSeer_results_ClosingStock_df['Product'] = FarSeer_results_ClosingStock_df['Product'].str.replace("'","")
    FarSeer_results_ClosingStock_df['Closing_Stock'] = FarSeer_results_ClosingStock_df['Closing_Stock'].astype(int)

    # Construct Triplet [Product - Month_Name - Stock_Value] #7 // Calculated Closing Stock Value per Month
    FarSeer_results_StockValue_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Stock_value'].copy()
    FarSeer_results_StockValue_df = FarSeer_results_StockValue_df.drop(columns=['Variable'])
    FarSeer_results_StockValue_df = FarSeer_results_StockValue_df.rename(columns={'index1': 'Product', 'Value': 'Stock_Value'})
    FarSeer_results_StockValue_df['Product'] = FarSeer_results_StockValue_df['Product'].str.replace("'","")
    FarSeer_results_StockValue_df['Stock_Value'] = FarSeer_results_StockValue_df['Stock_Value'].astype(int)

    # Construct Triplet [Product - Month_Name - Turnover] #8 // Calculated Turnover Value per Month
    FarSeer_results_Turnover_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Turnover'].copy()
    FarSeer_results_Turnover_df = FarSeer_results_Turnover_df.drop(columns=['Variable'])
    FarSeer_results_Turnover_df = FarSeer_results_Turnover_df.rename(columns={'index1': 'Product', 'Value': 'Turnover'})
    FarSeer_results_Turnover_df['Product'] = FarSeer_results_Turnover_df['Product'].str.replace("'","")
    FarSeer_results_Turnover_df['Turnover'] = FarSeer_results_Turnover_df['Turnover'].astype(int)

    # Construct Triplet [Product - Month_Name - Variable_Cost] #9 // Calculated Total Variable Cost per Month
    FarSeer_results_VariableCost_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Variable_costcalculated'].copy()
    FarSeer_results_VariableCost_df = FarSeer_results_VariableCost_df.drop(columns=['Variable'])
    FarSeer_results_VariableCost_df = FarSeer_results_VariableCost_df.rename(columns={'index1': 'Product', 'Value': 'Variable_Cost'})
    FarSeer_results_VariableCost_df['Product'] = FarSeer_results_VariableCost_df['Product'].str.replace("'","")
    FarSeer_results_VariableCost_df['Variable_Cost'] = FarSeer_results_VariableCost_df['Variable_Cost'].astype(int)

    # Construct Triplet [Product - Month_Name - Gross_Margin] #10 // Calculated Total Gross Margin per Month
    FarSeer_results_GrossMargin_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Gross_margin'].copy()
    FarSeer_results_GrossMargin_df = FarSeer_results_GrossMargin_df.drop(columns=['Variable'])
    FarSeer_results_GrossMargin_df = FarSeer_results_GrossMargin_df.rename(columns={'index1': 'Product', 'Value': 'Gross_Margin'})
    FarSeer_results_GrossMargin_df['Product'] = FarSeer_results_GrossMargin_df['Product'].str.replace("'","")
    FarSeer_results_GrossMargin_df['Gross_Margin'] = FarSeer_results_GrossMargin_df['Gross_Margin'].astype(int)

    # Construct Triplet [Product - Month_Name - Forecast_Forced] #11 // Retrieve Forecast Forced Sales Volumes
    FarSeer_results_Forecast_Forced_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Forecast_Forced'].copy()
    FarSeer_results_Forecast_Forced_df = FarSeer_results_Forecast_Forced_df.drop(columns=['Variable'])
    FarSeer_results_Forecast_Forced_df = FarSeer_results_Forecast_Forced_df.rename(columns={'index1': 'Product', 'Value': 'Forecast_Forced'})
    FarSeer_results_Forecast_Forced_df['Product'] = FarSeer_results_Forecast_Forced_df['Product'].str.replace("'","")
    FarSeer_results_Forecast_Forced_df['Forecast_Forced'] = FarSeer_results_Forecast_Forced_df['Forecast_Forced'].astype(int)

    # Construct Triplet [Product - Month_Name - Forecast_NonEssential] #12 // Retrieve Forecast Non Essential Sales Volumes
    FarSeer_results_Forecast_NonEssential_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Forecast_NonEssential'].copy()
    FarSeer_results_Forecast_NonEssential_df = FarSeer_results_Forecast_NonEssential_df.drop(columns=['Variable'])
    FarSeer_results_Forecast_NonEssential_df = FarSeer_results_Forecast_NonEssential_df.rename(columns={'index1': 'Product', 'Value': 'Forecast_NonEssential'})
    FarSeer_results_Forecast_NonEssential_df['Product'] = FarSeer_results_Forecast_NonEssential_df['Product'].str.replace("'","")
    FarSeer_results_Forecast_NonEssential_df['Forecast_NonEssential'] = FarSeer_results_Forecast_NonEssential_df['Forecast_NonEssential'].astype(int)

    # Construct Triplet [Product - Month_Name - Stock_Variation] #13 // Calculated Stock Variation per Month in Volume
    FarSeer_results_Stock_Variation_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Stock_variation'].copy()
    FarSeer_results_Stock_Variation_df = FarSeer_results_Stock_Variation_df.drop(columns=['Variable'])
    FarSeer_results_Stock_Variation_df = FarSeer_results_Stock_Variation_df.rename(columns={'index1': 'Product', 'Value': 'Stock_Variation'})
    FarSeer_results_Stock_Variation_df['Product'] = FarSeer_results_Stock_Variation_df['Product'].str.replace("'","")
    FarSeer_results_Stock_Variation_df['Stock_Variation'] = FarSeer_results_Stock_Variation_df['Stock_Variation'].astype(int)

    # Construct Triplet [Product - Month_Name - Product_Production] #14 // Calculated Product Production per Month in Volume
    FarSeer_results_Product_Production_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Product_production'].copy()
    FarSeer_results_Product_Production_df = FarSeer_results_Product_Production_df.drop(columns=['Variable'])
    FarSeer_results_Product_Production_df = FarSeer_results_Product_Production_df.rename(columns={'index1': 'Product', 'Value': 'Product_Production'})
    FarSeer_results_Product_Production_df['Product'] = FarSeer_results_Product_Production_df['Product'].str.replace("'","")
    FarSeer_results_Product_Production_df['Product_Production'] = FarSeer_results_Product_Production_df['Product_Production'].astype(int)

    # Construct Triplet [Product - Month_Name - StockVarValue] #15 // Calculated Stock Variation per Month in Value
    FarSeer_results_Stock_Variation_Value_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'StockVarValue'].copy()
    FarSeer_results_Stock_Variation_Value_df = FarSeer_results_Stock_Variation_Value_df.drop(columns=['Variable'])
    FarSeer_results_Stock_Variation_Value_df = FarSeer_results_Stock_Variation_Value_df.rename(columns={'index1': 'Product', 'Value': 'Stock_Variation_Value'})
    FarSeer_results_Stock_Variation_Value_df['Product'] = FarSeer_results_Stock_Variation_Value_df['Product'].str.replace("'","")
    FarSeer_results_Stock_Variation_Value_df['Stock_Variation_Value'] = FarSeer_results_Stock_Variation_Value_df['Stock_Variation_Value'].astype(int)

    # Merging Dataframe #1 with Dataframe #2
    FarSeer_results_df = pd.merge(FarSeer_results_SalesForced_df, FarSeer_results_ASP_Forced_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #3
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_SalesNonEssential_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #4
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_ASP_NonEssential_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #5
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_OpeningStock_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #6
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_ClosingStock_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #7
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_StockValue_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #8
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_Turnover_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #9
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_VariableCost_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #10
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_GrossMargin_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #11
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_Forecast_Forced_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #12
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_Forecast_NonEssential_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #13
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_Stock_Variation_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #14
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_Product_Production_df, on=['Product', 'Month_Name'], how='inner')
    # Adding Dataframe #15
    FarSeer_results_df = pd.merge(FarSeer_results_df, FarSeer_results_Stock_Variation_Value_df, on=['Product', 'Month_Name'], how='inner')


    # === Create Opening / Operating Hours Table per Scenario ===

    # Retrieve capacityModes_df #1
    capacityModes_df_json = quarry.capacityModes_df_json
    capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
    capacityModes_df['Coefficient'] = capacityModes_df['Coefficient'] / 100
    capacityModes_df['Coefficient'] = capacityModes_df['Coefficient'].astype(float).round(2)

    # Construct Triplet [Modes - Month_Name - Opening_Hours] #2
    FarSeer_results_OpeningHours_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Opening_hours'].copy()
    FarSeer_results_OpeningHours_df = FarSeer_results_OpeningHours_df.drop(columns=['Variable'])
    FarSeer_results_OpeningHours_df = FarSeer_results_OpeningHours_df.rename(columns={'index1': 'Modes', 'Value': 'Opening_Hours'})
    FarSeer_results_OpeningHours_df['Modes'] = FarSeer_results_OpeningHours_df['Modes'].str.replace("'","")
    FarSeer_results_OpeningHours_df['Opening_Hours'] = FarSeer_results_OpeningHours_df['Opening_Hours'].astype(float).round(1)

    # Construct Triplet [Modes - Month_Name - Operating_Hours] #3
    FarSeer_results_OperatingHours_df = FarSeer_raw_results_df[FarSeer_raw_results_df['Variable'] == 'Operating_hours'].copy()
    FarSeer_results_OperatingHours_df = FarSeer_results_OperatingHours_df.drop(columns=['Variable'])
    FarSeer_results_OperatingHours_df = FarSeer_results_OperatingHours_df.rename(columns={'index1': 'Modes', 'Value': 'Operating_Hours'})
    FarSeer_results_OperatingHours_df['Modes'] = FarSeer_results_OperatingHours_df['Modes'].str.replace("'","")
    FarSeer_results_OperatingHours_df['Operating_Hours'] = FarSeer_results_OperatingHours_df['Operating_Hours'].astype(float).round(1)

    # Retrieve maxOpeningHours_df #4
    maxOpeningHours_df_json = quarry.maxOpeningHours_df_json
    maxOpeningHours_df = pd.read_json(StringIO(maxOpeningHours_df_json), orient='records')
    maxOpeningHours_df['Month_Name'] = maxOpeningHours_df['Month'].map({v: k for k, v in month_to_number.items()})

    # Merging Dataframe #1 with Dataframe #2
    FarSeer_hours_df = pd.merge(capacityModes_df, FarSeer_results_OpeningHours_df, on=['Modes'], how='inner')
    # Adding Dataframe #3
    FarSeer_hours_df = pd.merge(FarSeer_hours_df, FarSeer_results_OperatingHours_df, on=['Modes', 'Month_Name'], how='inner')
    # Adding Dataframe #4
    FarSeer_hours_df = pd.merge(FarSeer_hours_df, maxOpeningHours_df, on=['Month_Name'], how='inner')

    return FarSeer_results_df, FarSeer_hours_df


def FarSeer_TO_Chart(project_data, dataframe):
    currency = project_data.currency

    # Select columns
    dataframe = dataframe[['Month_Name', 'Turnover', 'Variable_Cost', 'Gross_Margin', 'Stock_Variation', 'Product_Production']]

    # Grouping by specified columns and aggregating
    dataframe = dataframe.groupby(['Month_Name']).agg({
        'Turnover': 'sum',
        'Variable_Cost': 'sum',
        'Gross_Margin': 'sum',
        'Stock_Variation': 'sum',
        'Product_Production': 'sum'
    }).reset_index()

    dataframe['Month'] = dataframe['Month_Name'].map(month_to_number)
    dataframe.sort_values(by='Month', inplace=True)

    # Create trace for Turnover as waterfall chart
    trace_turnover = go.Waterfall(
        name="",
        orientation="v", # Vertical orientation
        x=dataframe['Month_Name'],
        y=dataframe['Turnover']/1000,
        increasing={
            'marker': {'color': '#2dce89'},
        },
        decreasing={
            'marker': {'color': '#f5365c'},
        },
    )


    # Create layout
    layout = go.Layout(
        title='Turnover Evolution per Month',
        title_font=dict(
            family='Segoe UI',  # Specify font family
            size=24,  # Specify font size
            color='#172b4d',  # Specify the font color
        ),
        yaxis=dict(
            title=f"Turnover [{currency}]",
            titlefont=dict(
                family='Segoe UI',  # Specify axis title font family
                size=18,  # Specify axis title font size
                color='#172b4d'  # Specify axis title font color
            ),
            tickformat=',.0f',  # Format tick labels to remove decimal places
            ticksuffix='k',  # Add 'k' as suffix to tick labels
            tickfont=dict(
                family='Segoe UI',  # Specify axis title font family
                size=14,  # Specify axis title font size
                color='#172b4d'  # Specify the font color
            ),
        gridcolor = '#e3e3e3',  # Specify color of x-axis gridlines
        gridwidth = 1,  # Specify width of x-axis gridlines
        zerolinecolor = '#e3e3e3',
        ),
        xaxis=dict(
            tickfont=dict(
                family="Segoe UI",
                size=18,
                color="#172b4d"
            ),
        gridcolor = '#e3e3e3',  # Specify color of x-axis gridlines
        gridwidth = 1,  # Specify width of x-axis gridlines
        zerolinecolor = '#e3e3e3',
        ),

        title_x = 0.5,  # Set the title's x position to center
        title_y = 0.9,  # Set the title's y position to slightly higher (adjust as needed)
        plot_bgcolor='#f6f9fc',
        paper_bgcolor='#f6f9fc',  # Set background color around the chart
    )

    # Create figure
    fig = go.Figure(trace_turnover, layout)

    # Convert the figure to HTML
    TO_Chart = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})

    return TO_Chart

def FarSeer_VC_Chart(project_data, dataframe):
    currency = project_data.currency
    # Select columns
    dataframe = dataframe[['Month_Name', 'Turnover', 'Variable_Cost', 'Gross_Margin', 'Stock_Variation', 'Product_Production', 'Stock_Variation_Value']]

    # Grouping by specified columns and aggregating
    dataframe = dataframe.groupby(['Month_Name']).agg({
        'Turnover': 'sum',
        'Variable_Cost': 'sum',
        'Gross_Margin': 'sum',
        'Stock_Variation': 'sum',
        'Product_Production': 'sum',
        'Stock_Variation_Value': 'sum',
    }).reset_index()

    dataframe['Month'] = dataframe['Month_Name'].map(month_to_number)
    dataframe.sort_values(by='Month', inplace=True)

    def calculate_VC_Sales(row):
        if row['Stock_Variation_Value'] > 0:
            return row['Variable_Cost'] - row['Stock_Variation_Value']
        else:
            return row['Variable_Cost']

    dataframe['VC_Sales'] = dataframe.apply(calculate_VC_Sales, axis=1)
    dataframe['VC_Stock'] = dataframe['Variable_Cost'] - dataframe['VC_Sales']

    # Create traces for each component of the bar chart
    trace_sales = go.Bar(
        name="",
        x=dataframe['Month_Name'],
        y=dataframe['VC_Sales'] / 1000,
        hoverinfo="text",
        text=['Variable Cost related to Sales: {:.1f}k'.format(vc_sales) for vc_sales in dataframe['VC_Sales'] / 1000],
        marker = dict(color='#2dce89'),  # Set the color to green

    )

    trace_stock = go.Bar(
        name="",
        x=dataframe['Month_Name'],
        y=dataframe['VC_Stock']/1000,
        hoverinfo="text",
        text=['Variable Cost related to Stock: {:.1f}k'.format(vc_stock) for vc_stock in dataframe['VC_Stock']/1000],
        marker=dict(color='#f5365c')  # Set the color to red
    )

    # Create layout
    layout = go.Layout(
        title=dict(
            text='Variable Cost Value per Month',
            font=dict(
                family='Segoe UI',
                size=24,  # Adjust the font size as needed
                color='#172b4d'
            )
        ),
        yaxis=dict(
            title=f"Variable Cost [{currency}]",
            titlefont=dict(
                family='Segoe UI',  # Specify axis title font family
                size=18,  # Specify axis title font size
                color='#172b4d'  # Specify axis title font color
            ),
            tickformat=',.0f',  # Format tick labels to remove decimal places
            ticksuffix='k',  # Add 'k' as suffix to tick labels
            tickfont = dict(
                family='Segoe UI',  # Specify axis title font family
                size=14,  # Specify axis title font size
                color='#172b4d'  # Specify the font color
            ),
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',
        ),

        xaxis=dict(
            tickfont=dict(
                family="Segoe UI",
                size=18,
                color="#172b4d"
            ),
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',
        ),

        barmode='stack',  # Set the bar mode to 'stack' to stack the bars on top of each other
        title_x=0.5,  # Set the title's x position to center
        title_y=0.9,  # Set the title's y position to slightly higher (adjust as needed)
        plot_bgcolor='#f6f9fc',
        paper_bgcolor='#f6f9fc',
        showlegend=False  # Hide the legend
    )

    # Create figure with both traces
    fig = go.Figure([trace_sales, trace_stock], layout)

    # Convert the figure to HTML
    VariableCost_Chart = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})

    return VariableCost_Chart


def FarSeer_GM_Chart(project_data, dataframe):
    currency = project_data.currency
    # Select columns
    dataframe = dataframe[['Month_Name', 'Turnover', 'Variable_Cost', 'Gross_Margin', 'Stock_Variation', 'Product_Production',]]

    # Grouping by specified columns and aggregating
    dataframe = dataframe.groupby(['Month_Name']).agg({
        'Turnover': 'sum',
        'Variable_Cost': 'sum',
        'Gross_Margin': 'sum',
        'Stock_Variation': 'sum',
        'Product_Production': 'sum'
    }).reset_index()

    dataframe['Month'] = dataframe['Month_Name'].map(month_to_number)
    dataframe.sort_values(by='Month', inplace=True)

    # Create trace for Gross_Margin as waterfall chart
    trace_gross_margin = go.Waterfall(
        name="",
        orientation="v", # Vertical orientation
        x=dataframe['Month_Name'],
        y=dataframe['Gross_Margin']/1000,
        increasing={'marker': {'color': '#2dce89'}},  # Set the color for increasing bars to green
        decreasing={'marker': {'color': '#f5365c'}},  # Set the color for decreasing bars to red
    )

    # Create layout
    layout = go.Layout(
        title='Gross Margin Evolution per Month',
        title_font=dict(
            family='Segoe UI',  # Specify font family
            size=24,  # Specify font size
            color='#172b4d',  # Specify the font color
        ),

        yaxis=dict(
            title=f"Gross Margin [{currency}]",
            titlefont=dict(
                family='Segoe UI',  # Specify axis title font family
                size=18,  # Specify axis title font size
                color='#172b4d'  # Specify axis title font color
            ),
            tickformat=',.0f',  # Format tick labels to remove decimal places
            ticksuffix='k',  # Add 'k' as suffix to tick labels
            tickfont=dict(
                family='Segoe UI',  # Specify axis title font family
                size=14,  # Specify axis title font size
                color='#172b4d'  # Specify the font color
            ),
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',
        ),

        xaxis=dict(
            tickfont=dict(
                family="Segoe UI",
                size=18,
                color="#172b4d"
            ),
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',
        ),

        title_x=0.5,  # Set the title's x position to center
        title_y=0.9,  # Set the title's y position to slightly higher (adjust as needed)
        plot_bgcolor='#f6f9fc',
        paper_bgcolor='#f6f9fc',  # Set background color around the chart
    )

    # Create figure
    fig = go.Figure(trace_gross_margin, layout)

    # Convert the figure to HTML
    GM_Chart = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})

    return GM_Chart

def FarSeer_Stock_Value_Chart(project_data, dataframe):
    currency = project_data.currency
    # Select columns
    dataframe = dataframe[['Month_Name', 'Turnover', 'Variable_Cost', 'Gross_Margin', 'Stock_Variation', 'Product_Production', 'Stock_Value']]

    # Grouping by specified columns and aggregating
    dataframe = dataframe.groupby(['Month_Name']).agg({
        'Turnover': 'sum',
        'Variable_Cost': 'sum',
        'Gross_Margin': 'sum',
        'Stock_Variation': 'sum',
        'Product_Production': 'sum',
        'Stock_Value': 'sum',
    }).reset_index()

    dataframe['Month'] = dataframe['Month_Name'].map(month_to_number)
    dataframe.sort_values(by='Month', inplace=True)

    # Create trace for Stock_Value as waterfall chart
    trace_stock_value = go.Bar(
        name="",
        orientation="v", # Vertical orientation
        x=dataframe['Month_Name'],
        y=dataframe['Stock_Value']/1000,
        marker=dict(color='#172b4d')  # Set the color to blue

    )

    # Create layout
    layout = go.Layout(
        title='Stock Value per Month',
        title_font=dict(
            family='Segoe UI',  # Specify font family
            size=24,  # Specify font size
            color='#172b4d',  # Specify the font color
        ),

        yaxis=dict(
            title=f"Stock Value [{currency}]",
            titlefont=dict(
                family='Segoe UI',  # Specify axis title font family
                size=18,  # Specify axis title font size
                color='#172b4d'  # Specify axis title font color
            ),
            tickmode='auto',  # Set tick mode to linear
            tick0=0,  # Start tick at 0
            dtick=1,  # Set tick interval to 1000
            tickformat=',.0f',  # Format tick labels to remove decimal places
            ticksuffix='k',  # Add 'k' as suffix to tick labels
            tickfont=dict(
                family='Segoe UI',  # Specify axis title font family
                size=14,  # Specify axis title font size
                color='#172b4d'  # Specify the font color
            ),
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',
        ),

        xaxis=dict(
            tickfont=dict(
                family="Segoe UI",
                size=18,
                color="#172b4d"
            ),
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',
        ),

        title_x = 0.5,  # Set the title's x position to center
        title_y = 0.9,  # Set the title's y position to slightly higher (adjust as needed)
        plot_bgcolor='#f6f9fc',
        paper_bgcolor='#f6f9fc',  # Set background color around the chart
    )

    # Create figure
    fig = go.Figure(trace_stock_value, layout)

    # Convert the figure to HTML
    Stock_Value_Chart = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})

    return Stock_Value_Chart


def FarSeer_Summary_Tables(FarSeer_meetAllSales_results_df, FarSeer_meetForcedSales_results_df, FarSeer_maxGM_results_df, quarry):
    # Retrieve salesForcast_df
    salesForecast_df_json = quarry.salesForecast_df_json
    salesForecast_df = pd.read_json(StringIO(salesForecast_df_json), orient='records')

    # Retrieve FarSeer_stockInfo_df
    FarSeer_stockInfo_df_json = quarry.FarSeer_stockInfo_df_json
    FarSeer_stockInfo_df = pd.read_json(StringIO(FarSeer_stockInfo_df_json), orient='records')

    # Initiate dictionary and calculate sum of Stock_Final_Month
    FarSeer_FinalStockRequirement = {}
    FarSeer_FinalStockRequirement['Stock_Final_Month'] = FarSeer_stockInfo_df['Stock_Final_Month'].sum()



    # Recover Optimisation Solution Data
    FarSeer_meetAllSales_results_df['Month'] = FarSeer_meetAllSales_results_df['Month_Name'].map(month_to_number)
    FarSeer_meetAllSales_results_df = FarSeer_meetAllSales_results_df[['Sales_Forced', 'Sales_Non_Essential', 'Closing_Stock', 'Stock_Value', 'Turnover', 'Gross_Margin', 'Month']]
    FarSeer_meetForcedSales_results_df['Month'] = FarSeer_meetForcedSales_results_df['Month_Name'].map(month_to_number)
    FarSeer_meetForcedSales_results_df = FarSeer_meetForcedSales_results_df[['Sales_Forced', 'Sales_Non_Essential', 'Closing_Stock', 'Stock_Value', 'Turnover', 'Gross_Margin', 'Month']]
    FarSeer_maxGM_results_df['Month'] = FarSeer_maxGM_results_df['Month_Name'].map(month_to_number)
    FarSeer_maxGM_results_df = FarSeer_maxGM_results_df[['Sales_Forced', 'Sales_Non_Essential', 'Closing_Stock', 'Stock_Value', 'Turnover', 'Gross_Margin', 'Month']]

    # Initiate dictionaries
    summaryTableMeetAllSales = {}
    summaryTableMeetForcedSales = {}
    summaryTableMaxGM = {}

    # Find Forcast_Forced
    summaryTableMeetAllSales['Forecast_Forced'] = salesForecast_df['Sales_Volume_Forced'].sum()
    summaryTableMeetForcedSales['Forecast_Forced'] = salesForecast_df['Sales_Volume_Forced'].sum()
    summaryTableMaxGM['Forecast_Forced'] = salesForecast_df['Sales_Volume_Forced'].sum()

    # Find Sales_Forced
    summaryTableMeetAllSales['Sales_Forced'] = FarSeer_meetAllSales_results_df['Sales_Forced'].sum()
    summaryTableMeetForcedSales['Sales_Forced'] = FarSeer_meetForcedSales_results_df['Sales_Forced'].sum()
    summaryTableMaxGM['Sales_Forced'] = FarSeer_maxGM_results_df['Sales_Forced'].sum()

    # Find Forecast_NonEssential
    summaryTableMeetAllSales['Forecast_NonEssential'] = salesForecast_df['Sales_Volume_Non_Essential'].sum()
    summaryTableMeetForcedSales['Forecast_NonEssential'] = salesForecast_df['Sales_Volume_Non_Essential'].sum()
    summaryTableMaxGM['Forecast_NonEssential'] = salesForecast_df['Sales_Volume_Non_Essential'].sum()

    # Find Sales_Non_Essential
    summaryTableMeetAllSales['Sales_Non_Essential'] = FarSeer_meetAllSales_results_df['Sales_Non_Essential'].sum()
    summaryTableMeetForcedSales['Sales_Non_Essential'] = FarSeer_meetForcedSales_results_df['Sales_Non_Essential'].sum()
    summaryTableMaxGM['Sales_Non_Essential'] = FarSeer_maxGM_results_df['Sales_Non_Essential'].sum()

    # Find Closing_Stock & Stock_Value
    largest_month_value = FarSeer_meetAllSales_results_df['Month'].max()
    MeetAllSales_temp_df = FarSeer_meetAllSales_results_df[FarSeer_meetAllSales_results_df['Month'] == largest_month_value]
    MeetForcedSales_temp_df = FarSeer_meetForcedSales_results_df[FarSeer_meetForcedSales_results_df['Month'] == largest_month_value]
    MaxGM_temp_df = FarSeer_maxGM_results_df[FarSeer_maxGM_results_df['Month'] == largest_month_value]

    summaryTableMeetAllSales['Closing_Stock'] = MeetAllSales_temp_df['Closing_Stock'].sum()
    summaryTableMeetForcedSales['Closing_Stock'] = MeetForcedSales_temp_df['Closing_Stock'].sum()
    summaryTableMaxGM['Closing_Stock'] = MaxGM_temp_df['Closing_Stock'].sum()

    summaryTableMeetAllSales['Stock_Value'] = MeetAllSales_temp_df['Stock_Value'].sum()
    summaryTableMeetForcedSales['Stock_Value'] = MeetForcedSales_temp_df['Stock_Value'].sum()
    summaryTableMaxGM['Stock_Value'] = MaxGM_temp_df['Stock_Value'].sum()

    # Find Turnover
    summaryTableMeetAllSales['Turnover'] = FarSeer_meetAllSales_results_df['Turnover'].sum()
    summaryTableMeetForcedSales['Turnover'] = FarSeer_meetForcedSales_results_df['Turnover'].sum()
    summaryTableMaxGM['Turnover'] = FarSeer_maxGM_results_df['Turnover'].sum()

    # Find Gross_Margin
    summaryTableMeetAllSales['Gross_Margin'] = FarSeer_meetAllSales_results_df['Gross_Margin'].sum()
    summaryTableMeetForcedSales['Gross_Margin'] = FarSeer_meetForcedSales_results_df['Gross_Margin'].sum()
    summaryTableMaxGM['Gross_Margin'] = FarSeer_maxGM_results_df['Gross_Margin'].sum()

    # Retrieve booleans for OptimisationSuccess and set them to True
    quarry.FarSeer_MeetAllSales_OptimisationSuccess = True
    quarry.FarSeer_MeetForcedSales_OptimisationSuccess = True
    quarry.FarSeer_MaxGM_OptimisationSuccess = True
    quarry.save()


    # Figure out whether solution is found with GLPK
    if summaryTableMeetAllSales['Forecast_Forced'] != 0 and summaryTableMeetAllSales['Sales_Forced'] == 0:
        quarry.FarSeer_MeetAllSales_OptimisationSuccess = False
        quarry.save()

    elif summaryTableMeetAllSales['Forecast_NonEssential'] != 0 and summaryTableMeetAllSales['Sales_Non_Essential'] == 0:
        quarry.FarSeer_MeetAllSales_OptimisationSuccess = False
        quarry.save()

    elif FarSeer_FinalStockRequirement['Stock_Final_Month'] !=0 and summaryTableMeetAllSales['Closing_Stock'] == 0:
        quarry.FarSeer_MeetAllSales_OptimisationSuccess = False
        quarry.save()

    if summaryTableMeetForcedSales['Forecast_Forced'] != 0 and summaryTableMeetForcedSales['Sales_Forced'] == 0:
        quarry.FarSeer_MeetForcedSales_OptimisationSuccess = False
        quarry.save()

    elif FarSeer_FinalStockRequirement['Stock_Final_Month'] !=0 and summaryTableMeetForcedSales['Closing_Stock'] == 0:
        quarry.FarSeer_MeetForcedSales_OptimisationSuccess = False
        quarry.save()

    if FarSeer_FinalStockRequirement['Stock_Final_Month'] !=0 and summaryTableMaxGM['Closing_Stock'] == 0:
        quarry.FarSeer_MaxGM_OptimisationSuccess = False
        quarry.save()


    # Retrieve booleans for OptimisationSuccess after conditions
    FarSeer_MeetAllSales_OptimisationSuccess = quarry.FarSeer_MeetAllSales_OptimisationSuccess
    FarSeer_MeetForcedSales_OptimisationSuccess = quarry.FarSeer_MeetForcedSales_OptimisationSuccess
    FarSeer_MaxGM_OptimisationSuccess = quarry.FarSeer_MaxGM_OptimisationSuccess


    # Apply format_with_commas function to all values
    for key, value in summaryTableMeetAllSales.items():
        summaryTableMeetAllSales[key] = format_with_commas(value)
    for key, value in summaryTableMeetForcedSales.items():
        summaryTableMeetForcedSales[key] = format_with_commas(value)
    for key, value in summaryTableMaxGM.items():
        summaryTableMaxGM[key] = format_with_commas(value)

    return summaryTableMeetAllSales, summaryTableMeetForcedSales, summaryTableMaxGM, FarSeer_MeetAllSales_OptimisationSuccess, FarSeer_MeetForcedSales_OptimisationSuccess, FarSeer_MaxGM_OptimisationSuccess