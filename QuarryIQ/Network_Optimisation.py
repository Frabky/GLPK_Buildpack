import pandas as pd
from io import StringIO
import numpy as np
import subprocess
import re
from itertools import product
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
from django.conf import settings

def format_with_commas(num):
    return "{:,.0f}".format(num)

def Calculate_Transport_Cost_Radius(project_data, radiusTable_df):
    # Retrieve dataframes
    distances_df_json = project_data.distances_df_json
    distances_df = pd.read_json(StringIO(distances_df_json), orient='records')

    distances_df['Distance'] = distances_df['Distance']/1000    # Transform distances from m to km

    # Create a copy of distances_df to store the results
    transport_cost_df = distances_df.copy()

    # Iterate through each row in radiusTable_df
    for index, row in radiusTable_df.iterrows():
        radius_name = row['Radius']
        from_distance = row['From']
        to_distance = row['To']

        # Calculate the distance within the current radius slice
        transport_cost_df[radius_name] = transport_cost_df['Distance'].apply(
            lambda distance: max(min(distance - from_distance, to_distance - from_distance), 0))

    # Iterate over each radius in radiusTable_df
    for _, row in radiusTable_df.iterrows():
        # Calculate the distance within the current radius slice
        transport_cost_df[row['Radius']] = transport_cost_df[row['Radius']] * row['Transport_Cost']

    # Filter out the radius columns
    radius_columns = [col for col in transport_cost_df.columns if col.startswith('Radius')]

    # Create a new column called 'Transport_Cost_Radius' containing the sum of all radius columns
    transport_cost_df['Transport_Cost'] = transport_cost_df[radius_columns].sum(axis=1)

    # Drop the radius columns from the DataFrame
    transport_cost_df.drop(columns=radius_columns, inplace=True)

    return transport_cost_df


def Reduce_Customer_Amount(project_data):
    # Retrieve dataframe
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Keep relevant columns
    temp_df = modified_transactions_df[['Quarries', 'Incoterms', 'Location', 'Product', 'Customers', 'Sales_Volume', 'Revenue']]


    combinations = float('inf')  # Start with a very large number to ensure the loop runs at least once
    threshold_value = 0
    step_size = 10

    while combinations > 350000:
        # Step 1: Group by 'Customers' and sum the 'Sales_Volume'
        customer_sales = temp_df.groupby('Customers')['Sales_Volume'].sum()

        # Step 2: Identify large and small customers based on current threshold_value
        large_customers = customer_sales[customer_sales >= threshold_value].index
        small_customers = customer_sales[customer_sales < threshold_value].index

        # Step 3: Filter the original DataFrame
        large_customers_df = temp_df[temp_df['Customers'].isin(large_customers)]
        small_customers_df = temp_df[temp_df['Customers'].isin(small_customers)]

        # Grouping by specified columns and aggregating for large customers
        large_customers_df = large_customers_df.groupby(
            ['Quarries', 'Incoterms', 'Location', 'Product', 'Customers']).agg({
            'Sales_Volume': 'sum',
            'Revenue': 'sum'
        }).reset_index()

        # Keep relevant columns for small customers
        small_customers_df = small_customers_df[['Quarries', 'Incoterms', 'Location', 'Product', 'Sales_Volume', 'Revenue']]

        # Grouping by specified columns and aggregating for small customers
        small_customers_df = small_customers_df.groupby(
            ['Quarries', 'Incoterms', 'Location', 'Product']).agg({
            'Sales_Volume': 'sum',
            'Revenue': 'sum'
        }).reset_index()

        # Adding a label to small customers
        small_customers_df['Customers'] = "'" + 'Small_Customers' + '-*-*-' + small_customers_df['Location'].astype(str) + "'"

        # Concatenating large and small customers dataframes
        reduced_customers_df = pd.concat([large_customers_df, small_customers_df], ignore_index=True)

        # Calculating Average Selling Price (ASP)
        reduced_customers_df['ASP'] = ((reduced_customers_df['Revenue'] / reduced_customers_df['Sales_Volume']).astype(float)).round(2)

        # Calculate number of unique items for each column in reduced_customers_df
        unique_quarries = reduced_customers_df['Quarries'].nunique()
        unique_incoterms = reduced_customers_df['Incoterms'].nunique()
        unique_products = reduced_customers_df['Product'].nunique()
        unique_customers = reduced_customers_df['Customers'].nunique()

        combinations = unique_quarries * unique_incoterms * unique_products * unique_customers

        # Update threshold_value for the next iteration
        threshold_value += step_size

    # Once combinations drop below 300000, return the required dataframes
    return reduced_customers_df, small_customers_df




def NO_Create_dat(project_data, dat_file, reduced_customers_df, small_customers_df, Max_Opening_Hours_Max_Sales_Volume_df, Enough_capacity_df):
    from itertools import product  # import here because doesn't want to use it above for some reason I don't know...

    # Retrieve the list of quarries
    quarries = sorted(project_data.quarries.all(), key=lambda x: x.name)


    # === Variable Costs {q in QUARRIES, p in PRODUCTS} [EUR/t] ===

    # Initialize an empty list to hold DataFrames
    temp_variable_cost_df = []

    # Iterate over the quarries
    for quarry in quarries:
        # Load the JSON data into a DataFrame
        P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
        if P4PFullSummary_df_json:  # Check if the JSON is not empty
            P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
            # Add a new column 'Quarry' with the name of the quarry
            P4PFullSummary_df['Quarry'] = quarry.name
            temp_variable_cost_df.append(P4PFullSummary_df)

    # Put everything together
    Variable_Cost_df = pd.concat(temp_variable_cost_df, ignore_index=True)

    # Calculate Variable_Cost
    Variable_Cost_df['Variable_Cost'] = Variable_Cost_df['ASP'] - Variable_Cost_df['Agg_levy'] - Variable_Cost_df['GM']

    # Keep relevant columns
    Variable_Cost_df = Variable_Cost_df[['Quarry', 'Product', 'Variable_Cost']]

    # Retrieve unique quarries and products
    unique_quarries = Variable_Cost_df['Quarry'].unique()
    unique_products = Variable_Cost_df['Product'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_products)), columns=['Quarry', 'Product'])

    # Merge with the original DataFrame to get Variable_Cost values
    Variable_Cost_df = pd.merge(all_combinations, Variable_Cost_df, on=['Quarry', 'Product'], how='left')

    # Fill missing Variable_Cost with 1000
    Variable_Cost_df['Variable_Cost'] = Variable_Cost_df['Variable_Cost'].fillna(1000)



    # === Product Composition {q in QUARRIES, p in PRODUCTS, co in CONSTITUENTS} [%] ===

    # Initialize an empty list to hold DataFrames
    temp_product_composition_df = []

    # Iterate over the quarries
    for quarry in quarries:
        # Load the JSON data into a DataFrame
        productComposition_df_json = quarry.productComposition_df_json
        if productComposition_df_json:  # Check if the JSON is not empty
            productComposition_df = pd.read_json(StringIO(productComposition_df_json), orient='records')
            # Add a new column 'Quarry' with the name of the quarry
            productComposition_df['Quarry'] = quarry.name
            temp_product_composition_df.append(productComposition_df)

    # Put everything together
    Product_Composition_df = pd.concat(temp_product_composition_df, ignore_index=True)

    # Keep relevant columns
    Product_Composition_df = Product_Composition_df[['Quarry', 'Product', 'Constituent', 'Composition']]

    # Retrieve unique constituents
    unique_constituents = Product_Composition_df['Constituent'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_products, unique_constituents)), columns=['Quarry', 'Product', 'Constituent'])

    # Merge with the original DataFrame to get Product_Composition_df values
    Product_Composition_df = pd.merge(all_combinations, Product_Composition_df, on=['Quarry', 'Product', 'Constituent'], how='left')

    # Fill missing Product_Composition_df with 0
    Product_Composition_df['Composition'] = Product_Composition_df['Composition'].fillna(0)
    Product_Composition_df['Composition'] = Product_Composition_df['Composition'] / 100



    # === Production Balance {q in QUARRIES, m in MODES, co in CONSTITUENTS} [%] ===

    # Initialize an empty list to hold DataFrames
    temp_production_balance_df = []

    # Iterate over the quarries
    for quarry in quarries:
        # Load the JSON data into a DataFrame
        constituentModesBalance_df_json = quarry.constituentModesBalance_df_json
        if constituentModesBalance_df_json:  # Check if the JSON is not empty
            constituentModesBalance_df = pd.read_json(StringIO(constituentModesBalance_df_json), orient='records')
            # Add a new column 'Quarry' with the name of the quarry
            constituentModesBalance_df['Quarry'] = quarry.name
            temp_production_balance_df.append(constituentModesBalance_df)

    # Put everything together
    Production_Balance_df = pd.concat(temp_production_balance_df, ignore_index=True)

    # Keep relevant columns
    Production_Balance_df = Production_Balance_df[['Quarry', 'Modes', 'Constituent', 'Balance']]

    # Retrieve unique modes
    unique_modes = Production_Balance_df['Modes'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_modes, unique_constituents)),
                                    columns=['Quarry', 'Modes', 'Constituent'])

    # Merge with the original DataFrame to get Production_Balance_df values
    Production_Balance_df = pd.merge(all_combinations, Production_Balance_df, on=['Quarry', 'Modes', 'Constituent'], how='left')

    # Fill missing Production_Balance_df with 0
    Production_Balance_df['Balance'] = Production_Balance_df['Balance'].fillna(0)
    Production_Balance_df['Balance'] = Production_Balance_df['Balance'] / 100



    # === Throughput & Process Coefficient {q in QUARRIES, m in MODES} [t/h] & [%] ===

    # Initialize an empty list to hold DataFrames
    temp_Throughput_Coeff_df = []

    # Iterate over the quarries
    for quarry in quarries:
        # Load the JSON data into a DataFrame
        capacityModes_df_json = quarry.capacityModes_df_json
        if capacityModes_df_json:  # Check if the JSON is not empty
            capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
            # Add a new column 'Quarry' with the name of the quarry
            capacityModes_df['Quarry'] = quarry.name
            temp_Throughput_Coeff_df.append(capacityModes_df)

    # Put everything together
    Throughput_Coeff_df = pd.concat(temp_Throughput_Coeff_df, ignore_index=True)

    # Keep relevant columns
    Throughput_Coeff_df = Throughput_Coeff_df[['Quarry', 'Modes', 'Max_capacity', 'Coefficient']]

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_modes)),
                                    columns=['Quarry', 'Modes'])

    # Merge with the original DataFrame to get Production_Balance_df values
    Throughput_Coeff_df = pd.merge(all_combinations, Throughput_Coeff_df, on=['Quarry', 'Modes'], how='left')

    # Fill missing Throughput_Coeff_df with 0
    Throughput_Coeff_df['Max_capacity'] = Throughput_Coeff_df['Max_capacity'].fillna(0)
    Throughput_Coeff_df['Coefficient'] = Throughput_Coeff_df['Coefficient'].fillna(0)
    Throughput_Coeff_df['Coefficient'] = Throughput_Coeff_df['Coefficient'] / 100



    # === Max Opening Hours & Max Volumes {q in QUARRIES} [h] & [t] ===

    # Keep relevant columns
    Max_Opening_Hours_Max_Sales_Volume_df = Max_Opening_Hours_Max_Sales_Volume_df[['Quarry', 'Max_Opening_Hours', 'Max_Sales_Volume']]


    # === Market Demand {cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS} [t] ===

    # Keep relevant columns
    Market_Demand_df = reduced_customers_df[['Customers', 'Product', 'Incoterms', 'Sales_Volume']].copy()

    # Grouping by specified columns and aggregating
    Market_Demand_df = Market_Demand_df.groupby(['Customers', 'Product', 'Incoterms']).agg({
        'Sales_Volume': 'sum',
    }).reset_index()

    # Retrieve unique Customers and Incoterms
    unique_customers = Market_Demand_df['Customers'].unique()
    unique_incoterms = Market_Demand_df['Incoterms'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_customers, unique_products, unique_incoterms)),
                                    columns=['Customers', 'Product', 'Incoterms'])

    # Merge with the original DataFrame to get Market_Demand_df values
    Market_Demand_df = pd.merge(all_combinations, Market_Demand_df, on=['Customers', 'Product', 'Incoterms'], how='left')

    # Fill missing Market_Demand_df with 0
    Market_Demand_df['Sales_Volume'] = Market_Demand_df['Sales_Volume'].fillna(0)




    # === Transport Cost {q in QUARRIES, cu in CUSTOMERS} [EUR/t] ===

    # Retrieve dataframe
    transport_cost_df_json = project_data.transport_cost_df_json
    transport_cost_df = pd.read_json(StringIO(transport_cost_df_json), orient='records')

    # Get unique small_customers
    unique_small_customers = small_customers_df['Customers'].unique()

    # Create a DataFrame with all combinations of quarries and unique_small_customers
    all_combinations_small_customers = pd.DataFrame(list(product(unique_quarries, unique_small_customers)), columns=['Quarries', 'Customers'])

    # Extract Location in small_customers_df
    # Remove leading and trailing single quotes from the 'Customers' column
    all_combinations_small_customers['Customers'] = all_combinations_small_customers['Customers'].str.strip("'")
    # Extract the location that appears after the pattern -*-*- at the end of the 'Customers' string
    all_combinations_small_customers['Location'] = all_combinations_small_customers['Customers'].str.extract(r'\-\*\-\*\-(\d+)$')
    # Convert the extracted 'Location' to integer type if necessary
    all_combinations_small_customers['Location'] = all_combinations_small_customers['Location'].astype(str)
    # Put back ' '
    all_combinations_small_customers['Customers'] = "'" + all_combinations_small_customers['Customers'] + "'"

    # Extract Location in transport_cost_df
    # Remove leading and trailing single quotes from the 'Customers' column
    transport_cost_df['Customers'] = transport_cost_df['Customers'].str.strip("'")
    # Extract the location that appears after the pattern -*-*- at the end of the 'Customers' string
    #transport_cost_df['Location'] = transport_cost_df['Customers'].str.extract(r'\-\*\-\*\-(\d+)$')
    transport_cost_df['Location'] = transport_cost_df['Customers'].str.extract(r'(\d{5})$')
    # Convert the extracted 'Location' to integer type if necessary
    transport_cost_df['Location'] = transport_cost_df['Location'].astype(str)
    # Put back ' '
    transport_cost_df['Customers'] = "'" + transport_cost_df['Customers'] + "'"

    # Match Distance and Transport_Cost on Location and Quarries
    all_combinations_small_customers = pd.merge(all_combinations_small_customers, transport_cost_df[['Quarries', 'Location', 'Distance', 'Transport_Cost']], on=['Quarries', 'Location'], how='left')

    # Drop duplicates across all columns
    all_combinations_small_customers = all_combinations_small_customers.drop_duplicates()

    # Concatenate all_combinations_small_customers with transport_cost_df
    transport_cost_df = pd.concat([all_combinations_small_customers, transport_cost_df], ignore_index=True)

    # Create a DataFrame with all combinations of quarries and unique_customers
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_customers)), columns=['Quarries', 'Customers'])

    # Merge with the original DataFrame to get Transport_Cost_df values
    Transport_Cost_per_Customer_df = pd.merge(all_combinations, transport_cost_df, on=['Quarries', 'Customers'], how='left')




    # === Sales Price {cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS} [EUR/t] ===

    # Keep relevant columns
    Sales_Price_df = reduced_customers_df[['Customers', 'Product', 'Incoterms', 'Revenue', 'Sales_Volume']].copy()

    # Grouping by specified columns and aggregating
    Sales_Price_df = Sales_Price_df.groupby(['Customers', 'Product', 'Incoterms']).agg({
        'Revenue': 'sum',
        'Sales_Volume': 'sum',
    }).reset_index()

    # Calculate ASP with condition to handle Sales_Volume = 0
    Sales_Price_df['ASP'] = np.where(Sales_Price_df['Sales_Volume'] == 0,
                                     0,
                                     (Sales_Price_df['Revenue'] / Sales_Price_df['Sales_Volume']).astype(float)).round(2)

    # Keep relevant columns
    Sales_Price_df = Sales_Price_df[['Customers', 'Product', 'Incoterms', 'ASP']]

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_customers, unique_products, unique_incoterms)),
                                    columns=['Customers', 'Product', 'Incoterms'])

    # Merge with the original DataFrame to get Market_Demand_df values
    Sales_Price_df = pd.merge(all_combinations, Sales_Price_df, on=['Customers', 'Product', 'Incoterms'],
                                how='left')

    # Fill missing Market_Demand_df with 0
    Sales_Price_df['ASP'] = Sales_Price_df['ASP'].fillna(0)



    # === Initial Status {q in QUARRIES, cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS} [t] ===

    # Keep relevant columns
    Initial_Status_df = reduced_customers_df[['Quarries', 'Customers', 'Product', 'Incoterms', 'Sales_Volume']].copy()

    # Grouping by specified columns and aggregating
    Initial_Status_df = Initial_Status_df.groupby(['Quarries', 'Customers', 'Product', 'Incoterms']).agg({
        'Sales_Volume': 'sum',
    }).reset_index()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_customers, unique_products, unique_incoterms)),
                                    columns=['Quarries', 'Customers', 'Product', 'Incoterms'])

    # Merge with the original DataFrame to get Market_Demand_df values
    Initial_Status_df = pd.merge(all_combinations, Initial_Status_df, on=['Quarries', 'Customers', 'Product', 'Incoterms'], how='left')

    # Fill missing Market_Demand_df with 0
    Initial_Status_df['Sales_Volume'] = Initial_Status_df['Sales_Volume'].fillna(0)




    # === .dat Creation ===

    # Open the .dat file in write mode
    with open(dat_file, "w") as f:
        # SET QUARRIES
        f.write("set QUARRIES :=\n")
        for quarry in unique_quarries:
            f.write(f'"{quarry}"\n')
        f.write(";\n\n")

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
            f.write(f'{customer}\n')
        f.write(";\n\n")

        # SET INCOTERMS
        f.write("set INCOTERMS :=\n")
        for incoterm in unique_incoterms:
            f.write(f'"{incoterm}"\n')
        f.write(";\n\n")

        # SET EMPTY
        f.write("set EMPTY :=\n")
        f.write("e\n")
        f.write(";\n\n")

        # param Variable_cost {q in QUARRIES, p in PRODUCTS}
        f.write("param Variable_cost :=\n")
        for index, row in Variable_Cost_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Product"]}"\t{row["Variable_Cost"]} \n')
        f.write(";\n\n")

        # param Products_composition {q in QUARRIES, p in PRODUCTS, co in CONSTITUENTS}
        f.write("param Products_composition :=\n")
        for index, row in Product_Composition_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Product"]}"\t"{row["Constituent"]}"\t{row["Composition"]} \n')
        f.write(";\n\n")

        # param Production_balance {q in QUARRIES, m in MODES, co in CONSTITUENTS}
        f.write("param Production_balance :=\n")
        for index, row in Production_Balance_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Modes"]}"\t"{row["Constituent"]}"\t{row["Balance"]} \n')
        f.write(";\n\n")

        # param Throughput {q in QUARRIES, m in MODES}
        f.write("param Throughput :=\n")
        for index, row in Throughput_Coeff_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Modes"]}"\t{row["Max_capacity"]} \n')
        f.write(";\n\n")

        # param Process_coefficient {q in QUARRIES, m in MODES}
        f.write("param Process_coefficient :=\n")
        for index, row in Throughput_Coeff_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Modes"]}"\t{row["Coefficient"]} \n')
        f.write(";\n\n")

        # param Maximum_opening_hours {q in QUARRIES}
        f.write("param Maximum_opening_hours :=\n")
        for index, row in Max_Opening_Hours_Max_Sales_Volume_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t{row["Max_Opening_Hours"]} \n')
        f.write(";\n\n")

        # param Maximum_volumes {q in QUARRIES}
        f.write("param Maximum_volumes :=\n")
        for index, row in Max_Opening_Hours_Max_Sales_Volume_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t{row["Max_Sales_Volume"]} \n')
        f.write(";\n\n")

        # param Market_demand {cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS}
        f.write("param Market_demand :=\n")
        for index, row in Market_Demand_df.iterrows():
            f.write(f'{row["Customers"]}\t"{row["Product"]}"\t"{row["Incoterms"]}"\t{row["Sales_Volume"]} \n')
        f.write(";\n\n")

        # param Transport_costs {q in QUARRIES, cu in CUSTOMERS}
        f.write("param Transport_costs :=\n")
        for index, row in Transport_Cost_per_Customer_df.iterrows():
            f.write(f'"{row["Quarries"]}"\t{row["Customers"]}\t{row["Transport_Cost"]} \n')
        f.write(";\n\n")

        # param Sales_price {cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS}
        f.write("param Sales_price :=\n")
        for index, row in Sales_Price_df.iterrows():
            f.write(f'{row["Customers"]}\t"{row["Product"]}"\t"{row["Incoterms"]}"\t{row["ASP"]} \n')
        f.write(";\n\n")

        # param Initial_status {q in QUARRIES, cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS}
        f.write("param Initial_status :=\n")
        for index, row in Initial_Status_df.iterrows():
            f.write(f'"{row["Quarries"]}"\t{row["Customers"]}\t"{row["Product"]}"\t"{row["Incoterms"]}"\t{row["Sales_Volume"]} \n')
        f.write(";\n\n")

        # param Enough_capacity {q in QUARRIES}
        f.write("param Enough_capacity :=\n")
        for index, row in Enough_capacity_df.iterrows():
            f.write(
                f'"{row["Quarries"]}"\t{row["Enough_Capacity"]} \n')
        f.write(";\n\n")

    return Transport_Cost_per_Customer_df



def NO_Create_MaxGM_dat(project_data, dat_file, reduced_customers_df, small_customers_df, NO_Scenario_Raw_Results_df):
    from itertools import product  # import here because doesn't want to use it above for some reason I don't know...

    # Recover NO_Scenario_Raw_Results_df from Meeting All Sales to get Best_Routes_df
    # Extract Sales Details per Customer
    Best_Routes_df = NO_Scenario_Raw_Results_df[NO_Scenario_Raw_Results_df['Variable'] == 'Sales'].copy()

    # Format Value
    Best_Routes_df['Value'] = Best_Routes_df['Value'].astype(float)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'index2': 'Customers',
        'index3': 'Product',
        'index4': 'Incoterms',
        'Value': 'Sales_Volume'
    }
    Best_Routes_df.rename(columns=columns_to_rename, inplace=True)

    # Drop Variable columns
    Best_Routes_df.drop(columns='Variable', inplace=True)

    # Delete ' '
    Best_Routes_df['Quarries'] = Best_Routes_df['Quarries'].replace({"'": ""}, regex=True)
    Best_Routes_df['Product'] = Best_Routes_df['Product'].replace({"'": ""}, regex=True)



    # Retrieve the list of quarries
    quarries = sorted(project_data.quarries.all(), key=lambda x: x.name)


    # === Variable Costs {q in QUARRIES, p in PRODUCTS} [EUR/t] ===

    # Initialize an empty list to hold DataFrames
    temp_variable_cost_df = []

    # Iterate over the quarries
    for quarry in quarries:
        # Load the JSON data into a DataFrame
        P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
        if P4PFullSummary_df_json:  # Check if the JSON is not empty
            P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
            # Add a new column 'Quarry' with the name of the quarry
            P4PFullSummary_df['Quarry'] = quarry.name
            temp_variable_cost_df.append(P4PFullSummary_df)

    # Put everything together
    Variable_Cost_df = pd.concat(temp_variable_cost_df, ignore_index=True)

    # Calculate Variable_Cost
    Variable_Cost_df['Variable_Cost'] = Variable_Cost_df['ASP'] - Variable_Cost_df['Agg_levy'] - Variable_Cost_df['GM']

    # Keep relevant columns
    Variable_Cost_df = Variable_Cost_df[['Quarry', 'Product', 'Variable_Cost']]

    # Retrieve unique quarries and products
    unique_quarries = Variable_Cost_df['Quarry'].unique()
    unique_products = Variable_Cost_df['Product'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_products)), columns=['Quarry', 'Product'])

    # Merge with the original DataFrame to get Variable_Cost values
    Variable_Cost_df = pd.merge(all_combinations, Variable_Cost_df, on=['Quarry', 'Product'], how='left')

    # Fill missing Variable_Cost with 1000
    Variable_Cost_df['Variable_Cost'] = Variable_Cost_df['Variable_Cost'].fillna(1000)



    # === Product Composition {q in QUARRIES, p in PRODUCTS, co in CONSTITUENTS} [%] ===

    # Initialize an empty list to hold DataFrames
    temp_product_composition_df = []

    # Iterate over the quarries
    for quarry in quarries:
        # Load the JSON data into a DataFrame
        productComposition_df_json = quarry.productComposition_df_json
        if productComposition_df_json:  # Check if the JSON is not empty
            productComposition_df = pd.read_json(StringIO(productComposition_df_json), orient='records')
            # Add a new column 'Quarry' with the name of the quarry
            productComposition_df['Quarry'] = quarry.name
            temp_product_composition_df.append(productComposition_df)

    # Put everything together
    Product_Composition_df = pd.concat(temp_product_composition_df, ignore_index=True)

    # Keep relevant columns
    Product_Composition_df = Product_Composition_df[['Quarry', 'Product', 'Constituent', 'Composition']]

    # Retrieve unique constituents
    unique_constituents = Product_Composition_df['Constituent'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_products, unique_constituents)), columns=['Quarry', 'Product', 'Constituent'])

    # Merge with the original DataFrame to get Product_Composition_df values
    Product_Composition_df = pd.merge(all_combinations, Product_Composition_df, on=['Quarry', 'Product', 'Constituent'], how='left')

    # Fill missing Product_Composition_df with 0
    Product_Composition_df['Composition'] = Product_Composition_df['Composition'].fillna(0)
    Product_Composition_df['Composition'] = Product_Composition_df['Composition'] / 100



    # === Production Balance {q in QUARRIES, m in MODES, co in CONSTITUENTS} [%] ===

    # Initialize an empty list to hold DataFrames
    temp_production_balance_df = []

    # Iterate over the quarries
    for quarry in quarries:
        # Load the JSON data into a DataFrame
        constituentModesBalance_df_json = quarry.constituentModesBalance_df_json
        if constituentModesBalance_df_json:  # Check if the JSON is not empty
            constituentModesBalance_df = pd.read_json(StringIO(constituentModesBalance_df_json), orient='records')
            # Add a new column 'Quarry' with the name of the quarry
            constituentModesBalance_df['Quarry'] = quarry.name
            temp_production_balance_df.append(constituentModesBalance_df)

    # Put everything together
    Production_Balance_df = pd.concat(temp_production_balance_df, ignore_index=True)

    # Keep relevant columns
    Production_Balance_df = Production_Balance_df[['Quarry', 'Modes', 'Constituent', 'Balance']]

    # Retrieve unique modes
    unique_modes = Production_Balance_df['Modes'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_modes, unique_constituents)),
                                    columns=['Quarry', 'Modes', 'Constituent'])

    # Merge with the original DataFrame to get Production_Balance_df values
    Production_Balance_df = pd.merge(all_combinations, Production_Balance_df, on=['Quarry', 'Modes', 'Constituent'], how='left')

    # Fill missing Production_Balance_df with 0
    Production_Balance_df['Balance'] = Production_Balance_df['Balance'].fillna(0)
    Production_Balance_df['Balance'] = Production_Balance_df['Balance'] / 100



    # === Throughput & Process Coefficient {q in QUARRIES, m in MODES} [t/h] & [%] ===

    # Initialize an empty list to hold DataFrames
    temp_Throughput_Coeff_df = []

    # Iterate over the quarries
    for quarry in quarries:
        # Load the JSON data into a DataFrame
        capacityModes_df_json = quarry.capacityModes_df_json
        if capacityModes_df_json:  # Check if the JSON is not empty
            capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
            # Add a new column 'Quarry' with the name of the quarry
            capacityModes_df['Quarry'] = quarry.name
            temp_Throughput_Coeff_df.append(capacityModes_df)

    # Put everything together
    Throughput_Coeff_df = pd.concat(temp_Throughput_Coeff_df, ignore_index=True)

    # Keep relevant columns
    Throughput_Coeff_df = Throughput_Coeff_df[['Quarry', 'Modes', 'Max_capacity', 'Coefficient']]

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_modes)),
                                    columns=['Quarry', 'Modes'])

    # Merge with the original DataFrame to get Production_Balance_df values
    Throughput_Coeff_df = pd.merge(all_combinations, Throughput_Coeff_df, on=['Quarry', 'Modes'], how='left')

    # Fill missing Throughput_Coeff_df with 0
    Throughput_Coeff_df['Max_capacity'] = Throughput_Coeff_df['Max_capacity'].fillna(0)
    Throughput_Coeff_df['Coefficient'] = Throughput_Coeff_df['Coefficient'].fillna(0)
    Throughput_Coeff_df['Coefficient'] = Throughput_Coeff_df['Coefficient'] / 100



    # === Max Opening Hours & Max Volumes {q in QUARRIES} [h] & [t] ===

    # Retrieve dataframe
    baseline_infoTable_df_json = project_data.baseline_infoTable_df_json
    baseline_infoTable_df = pd.read_json(StringIO(baseline_infoTable_df_json), orient='records')

    # Keep relevant columns
    Max_Opening_Hours_Max_Sales_Volume_df = baseline_infoTable_df[['Quarry', 'Max_Opening_Hours', 'Max_Sales_Volume']]



    # === Market Demand {cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS} [t] ===

    # Keep relevant columns
    Market_Demand_df = reduced_customers_df[['Customers', 'Product', 'Incoterms', 'Sales_Volume']].copy()

    # Grouping by specified columns and aggregating
    Market_Demand_df = Market_Demand_df.groupby(['Customers', 'Product', 'Incoterms']).agg({
        'Sales_Volume': 'sum',
    }).reset_index()

    # Retrieve unique Customers and Incoterms
    unique_customers = Market_Demand_df['Customers'].unique()
    unique_incoterms = Market_Demand_df['Incoterms'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_customers, unique_products, unique_incoterms)),
                                    columns=['Customers', 'Product', 'Incoterms'])

    # Merge with the original DataFrame to get Market_Demand_df values
    Market_Demand_df = pd.merge(all_combinations, Market_Demand_df, on=['Customers', 'Product', 'Incoterms'], how='left')

    # Fill missing Market_Demand_df with 0
    Market_Demand_df['Sales_Volume'] = Market_Demand_df['Sales_Volume'].fillna(0)




    # === Transport Cost {q in QUARRIES, cu in CUSTOMERS} [EUR/t] ===

    # Retrieve dataframe
    transport_cost_df_json = project_data.transport_cost_df_json
    transport_cost_df = pd.read_json(StringIO(transport_cost_df_json), orient='records')

    # Get unique small_customers
    unique_small_customers = small_customers_df['Customers'].unique()

    # Create a DataFrame with all combinations of quarries and unique_small_customers
    all_combinations_small_customers = pd.DataFrame(list(product(unique_quarries, unique_small_customers)), columns=['Quarries', 'Customers'])

    # Extract Location in small_customers_df
    # Remove leading and trailing single quotes from the 'Customers' column
    all_combinations_small_customers['Customers'] = all_combinations_small_customers['Customers'].str.strip("'")
    # Extract the location that appears after the pattern -*-*- at the end of the 'Customers' string
    all_combinations_small_customers['Location'] = all_combinations_small_customers['Customers'].str.extract(r'\-\*\-\*\-(\d+)$')
    # Convert the extracted 'Location' to integer type if necessary
    all_combinations_small_customers['Location'] = all_combinations_small_customers['Location'].astype(str)
    # Put back ' '
    all_combinations_small_customers['Customers'] = "'" + all_combinations_small_customers['Customers'] + "'"

    # Extract Location in transport_cost_df
    # Remove leading and trailing single quotes from the 'Customers' column
    transport_cost_df['Customers'] = transport_cost_df['Customers'].str.strip("'")
    # Extract the location that appears after the pattern -*-*- at the end of the 'Customers' string
    #transport_cost_df['Location'] = transport_cost_df['Customers'].str.extract(r'\-\*\-\*\-(\d+)$')
    transport_cost_df['Location'] = transport_cost_df['Customers'].str.extract(r'(\d{5})$')
    # Convert the extracted 'Location' to integer type if necessary
    transport_cost_df['Location'] = transport_cost_df['Location'].astype(str)
    # Put back ' '
    transport_cost_df['Customers'] = "'" + transport_cost_df['Customers'] + "'"

    # Match Distance and Transport_Cost on Location and Quarries
    all_combinations_small_customers = pd.merge(all_combinations_small_customers, transport_cost_df[['Quarries', 'Location', 'Distance', 'Transport_Cost']], on=['Quarries', 'Location'], how='left')

    # Drop duplicates across all columns
    all_combinations_small_customers = all_combinations_small_customers.drop_duplicates()

    # Concatenate all_combinations_small_customers with transport_cost_df
    transport_cost_df = pd.concat([all_combinations_small_customers, transport_cost_df], ignore_index=True)

    # Create a DataFrame with all combinations of quarries and unique_customers
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_customers)), columns=['Quarries', 'Customers'])

    # Merge with the original DataFrame to get Transport_Cost_df values
    Transport_Cost_per_Customer_df = pd.merge(all_combinations, transport_cost_df, on=['Quarries', 'Customers'], how='left')




    # === Sales Price {cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS} [EUR/t] ===

    # Keep relevant columns
    Sales_Price_df = reduced_customers_df[['Customers', 'Product', 'Incoterms', 'Revenue', 'Sales_Volume']].copy()

    # Grouping by specified columns and aggregating
    Sales_Price_df = Sales_Price_df.groupby(['Customers', 'Product', 'Incoterms']).agg({
        'Revenue': 'sum',
        'Sales_Volume': 'sum',
    }).reset_index()

    # Calculate ASP with condition to handle Sales_Volume = 0
    Sales_Price_df['ASP'] = np.where(Sales_Price_df['Sales_Volume'] == 0,
                                     0,
                                     (Sales_Price_df['Revenue'] / Sales_Price_df['Sales_Volume']).astype(float)).round(2)

    # Keep relevant columns
    Sales_Price_df = Sales_Price_df[['Customers', 'Product', 'Incoterms', 'ASP']]

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_customers, unique_products, unique_incoterms)),
                                    columns=['Customers', 'Product', 'Incoterms'])

    # Merge with the original DataFrame to get Market_Demand_df values
    Sales_Price_df = pd.merge(all_combinations, Sales_Price_df, on=['Customers', 'Product', 'Incoterms'],
                                how='left')

    # Fill missing Market_Demand_df with 0
    Sales_Price_df['ASP'] = Sales_Price_df['ASP'].fillna(0)



    # === Initial Status {q in QUARRIES, cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS} [t] ===

    # Keep relevant columns
    Initial_Status_df = reduced_customers_df[['Quarries', 'Customers', 'Product', 'Incoterms', 'Sales_Volume']].copy()

    # Grouping by specified columns and aggregating
    Initial_Status_df = Initial_Status_df.groupby(['Quarries', 'Customers', 'Product', 'Incoterms']).agg({
        'Sales_Volume': 'sum',
    }).reset_index()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_customers, unique_products, unique_incoterms)),
                                    columns=['Quarries', 'Customers', 'Product', 'Incoterms'])

    # Merge with the original DataFrame to get Market_Demand_df values
    Initial_Status_df = pd.merge(all_combinations, Initial_Status_df, on=['Quarries', 'Customers', 'Product', 'Incoterms'], how='left')

    # Fill missing Market_Demand_df with 0
    Initial_Status_df['Sales_Volume'] = Initial_Status_df['Sales_Volume'].fillna(0)




    # === .dat Creation ===

    # Open the .dat file in write mode
    with open(dat_file, "w") as f:
        # SET QUARRIES
        f.write("set QUARRIES :=\n")
        for quarry in unique_quarries:
            f.write(f'"{quarry}"\n')
        f.write(";\n\n")

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
            f.write(f'{customer}\n')
        f.write(";\n\n")

        # SET INCOTERMS
        f.write("set INCOTERMS :=\n")
        for incoterm in unique_incoterms:
            f.write(f'"{incoterm}"\n')
        f.write(";\n\n")

        # SET EMPTY
        f.write("set EMPTY :=\n")
        f.write("e\n")
        f.write(";\n\n")

        # param Variable_cost {q in QUARRIES, p in PRODUCTS}
        f.write("param Variable_cost :=\n")
        for index, row in Variable_Cost_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Product"]}"\t{row["Variable_Cost"]} \n')
        f.write(";\n\n")

        # param Products_composition {q in QUARRIES, p in PRODUCTS, co in CONSTITUENTS}
        f.write("param Products_composition :=\n")
        for index, row in Product_Composition_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Product"]}"\t"{row["Constituent"]}"\t{row["Composition"]} \n')
        f.write(";\n\n")

        # param Production_balance {q in QUARRIES, m in MODES, co in CONSTITUENTS}
        f.write("param Production_balance :=\n")
        for index, row in Production_Balance_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Modes"]}"\t"{row["Constituent"]}"\t{row["Balance"]} \n')
        f.write(";\n\n")

        # param Throughput {q in QUARRIES, m in MODES}
        f.write("param Throughput :=\n")
        for index, row in Throughput_Coeff_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Modes"]}"\t{row["Max_capacity"]} \n')
        f.write(";\n\n")

        # param Process_coefficient {q in QUARRIES, m in MODES}
        f.write("param Process_coefficient :=\n")
        for index, row in Throughput_Coeff_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t"{row["Modes"]}"\t{row["Coefficient"]} \n')
        f.write(";\n\n")

        # param Maximum_opening_hours {q in QUARRIES}
        f.write("param Maximum_opening_hours :=\n")
        for index, row in Max_Opening_Hours_Max_Sales_Volume_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t{row["Max_Opening_Hours"]} \n')
        f.write(";\n\n")

        # param Maximum_volumes {q in QUARRIES}
        f.write("param Maximum_volumes :=\n")
        for index, row in Max_Opening_Hours_Max_Sales_Volume_df.iterrows():
            f.write(f'"{row["Quarry"]}"\t{row["Max_Sales_Volume"]} \n')
        f.write(";\n\n")

        # param Market_demand {cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS}
        f.write("param Market_demand :=\n")
        for index, row in Market_Demand_df.iterrows():
            f.write(f'{row["Customers"]}\t"{row["Product"]}"\t"{row["Incoterms"]}"\t{row["Sales_Volume"]} \n')
        f.write(";\n\n")

        # param Transport_costs {q in QUARRIES, cu in CUSTOMERS}
        f.write("param Transport_costs :=\n")
        for index, row in Transport_Cost_per_Customer_df.iterrows():
            f.write(f'"{row["Quarries"]}"\t{row["Customers"]}\t{row["Transport_Cost"]} \n')
        f.write(";\n\n")

        # param Sales_price {cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS}
        f.write("param Sales_price :=\n")
        for index, row in Sales_Price_df.iterrows():
            f.write(f'{row["Customers"]}\t"{row["Product"]}"\t"{row["Incoterms"]}"\t{row["ASP"]} \n')
        f.write(";\n\n")

        # param Initial_status {q in QUARRIES, cu in CUSTOMERS, p in PRODUCTS, inc in INCOTERMS}
        f.write("param Best_routes :=\n")
        for index, row in Best_Routes_df.iterrows():
            f.write(f'"{row["Quarries"]}"\t{row["Customers"]}\t"{row["Product"]}"\t"{row["Incoterms"]}"\t{row["Sales_Volume"]} \n')
        f.write(";\n\n")




def NO_GLPK(model_file, dat_file, result_file):
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
    pattern = re.compile(
        r'(\w+)?\[(.+?)\]\s+(\w+)\s+([\d\.eE+-]+)?\s+([\d\.eE+-]+)?\s+([\d\.eE+-]+)?\s+([\d\.eE+-]+)?(?!\s*\d)')

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
    NO_raw_results_df = pd.DataFrame(variable_data)
    NO_raw_results_df = NO_raw_results_df.drop(columns=['index5'])

    return NO_raw_results_df





def Extract_Customer_Results_Baseline(project_data):
    # Extract Customer Transactions
    # Retrieve dataframes
    NO_Baseline_Raw_Results_df_json = project_data.NO_Baseline_Raw_Results_df_json
    NO_Baseline_Raw_Results_df = pd.read_json(StringIO(NO_Baseline_Raw_Results_df_json), orient='records')

    # Extract Sales Details per Customer
    Customer_Sales_df = NO_Baseline_Raw_Results_df[NO_Baseline_Raw_Results_df['Variable'] == 'Sales'].copy()

    # Format Value
    Customer_Sales_df['Value'] = Customer_Sales_df['Value'].astype(float)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'index2': 'Customers',
        'index3': 'Product',
        'index4': 'Incoterms',
        'Value': 'Sales_Volume'
    }
    Customer_Sales_df.rename(columns=columns_to_rename, inplace=True)

    # Drop Variable columns
    Customer_Sales_df.drop(columns='Variable', inplace=True)

    # Delete ' '
    Customer_Sales_df['Quarries'] = Customer_Sales_df['Quarries'].replace({"'": ""}, regex=True)
    Customer_Sales_df['Customers'] = Customer_Sales_df['Customers'].replace({"'": ""}, regex=True)
    Customer_Sales_df['Product'] = Customer_Sales_df['Product'].replace({"'": ""}, regex=True)

    # Remove rows where Sales_Volume is 0
    Customer_Sales_df = Customer_Sales_df[Customer_Sales_df['Sales_Volume'] != 0].reset_index(drop=True)



    # Extract Sales Price per Customer
    # Retrieve dataframes
    reduced_customers_df_json = project_data.reduced_customers_df_json
    reduced_customers_df = pd.read_json(StringIO(reduced_customers_df_json), orient='records')

    # Keep relevant columns
    reduced_customers_df = reduced_customers_df[['Quarries', 'Customers', 'Product', 'Incoterms', 'ASP']]
    # Delete ' '
    reduced_customers_df['Quarries'] = reduced_customers_df['Quarries'].replace({"'": ""}, regex=True)
    reduced_customers_df['Customers'] = reduced_customers_df['Customers'].replace({"'": ""}, regex=True)
    reduced_customers_df['Product'] = reduced_customers_df['Product'].replace({"'": ""}, regex=True)



    # Extract Product Variable Cost per Quarry
    # Retrieve all related Quarry instances for the specific ProjectData
    quarries = project_data.quarries.all()

    # Initialize an empty list to store individual DataFrames
    dfs = []

    # Iterate through each quarry
    for quarry in quarries:
        # Read JSON data into a DataFrame
        P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
        P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')

        # Add 'Quarries' column with quarry name
        P4PFullSummary_df.insert(0, 'Quarries', quarry.name)

        # Append DataFrame to the list
        dfs.append(P4PFullSummary_df)

    # Concatenate all DataFrames in the list vertically
    Variable_Cost_df = pd.concat(dfs, ignore_index=True)

    # Calculate Variable Cost
    Variable_Cost_df['Variable_Cost'] = Variable_Cost_df['ASP'] - Variable_Cost_df['Agg_levy'] - Variable_Cost_df['GM']

    # Keep relevant columns
    Variable_Cost_df = Variable_Cost_df[['Quarries', 'Product', 'Variable_Cost']]

    # Retrieve unique quarries and products
    unique_quarries = Variable_Cost_df['Quarries'].unique()
    unique_products = Variable_Cost_df['Product'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_products)), columns=['Quarries', 'Product'])

    # Merge with the original DataFrame to get Variable_Cost values
    Variable_Cost_df = pd.merge(all_combinations, Variable_Cost_df, on=['Quarries', 'Product'], how='left')

    # Fill missing Variable_Cost with 1000
    Variable_Cost_df['Variable_Cost'] = Variable_Cost_df['Variable_Cost'].fillna(1000)



    # Extract Transport_Cost_per_Customer
    # Retrieve dataframes
    Transport_Cost_per_Customer_df_json = project_data.Transport_Cost_per_Customer_df_json
    Transport_Cost_per_Customer_df = pd.read_json(StringIO(Transport_Cost_per_Customer_df_json), orient='records')

    # Delete ' '
    Transport_Cost_per_Customer_df['Quarries'] = Transport_Cost_per_Customer_df['Quarries'].replace({"'": ""}, regex=True)
    Transport_Cost_per_Customer_df['Customers'] = Transport_Cost_per_Customer_df['Customers'].replace({"'": ""}, regex=True)

    # Keep relevant columns
    Transport_Cost_per_Customer_df = Transport_Cost_per_Customer_df[['Quarries', 'Customers', 'Transport_Cost']]



    # Merge all Dataframes together
    # Merge for ASP
    NO_Baseline_Customer_Results_df = pd.merge(Customer_Sales_df, reduced_customers_df, on=['Quarries', 'Customers', 'Product', 'Incoterms'], how='left')
    # Merge for Variable Cost
    NO_Baseline_Customer_Results_df = pd.merge(NO_Baseline_Customer_Results_df, Variable_Cost_df, on=['Quarries', 'Product'], how='left')
    # Merge for Transport cost
    NO_Baseline_Customer_Results_df = pd.merge(NO_Baseline_Customer_Results_df, Transport_Cost_per_Customer_df, on=['Quarries', 'Customers'], how='left')

    # Transport = 0 if Incoterms = Ex-works
    # Define a function to apply the required logic
    def modify_transport_cost(row):
        if row['Incoterms'] == 'Ex-works':
            return 0
        return row['Transport_Cost']

    # Apply the function to the 'Transport_Cost' column
    NO_Baseline_Customer_Results_df['Transport_Cost'] = NO_Baseline_Customer_Results_df.apply(modify_transport_cost, axis=1)


    # Rename multiple columns
    columns_to_rename = {
        'ASP': 'Sales_Price',
        'Sales_Volume': 'Sales_Volume_Baseline',
        'Quarries': 'Quarries_Baseline',
        'Variable_Cost': 'Variable_Cost_per_t_Baseline',
        'Transport_Cost': 'Transport_Cost_per_t_Baseline',
    }
    NO_Baseline_Customer_Results_df.rename(columns=columns_to_rename, inplace=True)

    # Calculation
    NO_Baseline_Customer_Results_df['Gross_Revenue_per_t'] = NO_Baseline_Customer_Results_df['Sales_Price'] + NO_Baseline_Customer_Results_df['Transport_Cost_per_t_Baseline']
    NO_Baseline_Customer_Results_df['Gross_Revenue_Baseline'] = NO_Baseline_Customer_Results_df['Sales_Volume_Baseline'] * NO_Baseline_Customer_Results_df['Gross_Revenue_per_t']
    NO_Baseline_Customer_Results_df['Transport_Cost_Baseline'] = NO_Baseline_Customer_Results_df['Sales_Volume_Baseline'] * NO_Baseline_Customer_Results_df['Transport_Cost_per_t_Baseline']
    NO_Baseline_Customer_Results_df['Variable_Cost_Sales_Baseline'] = NO_Baseline_Customer_Results_df['Sales_Volume_Baseline'] * NO_Baseline_Customer_Results_df['Variable_Cost_per_t_Baseline']
    NO_Baseline_Customer_Results_df['Gross_Margin_per_t_Baseline'] = NO_Baseline_Customer_Results_df['Sales_Price'] - NO_Baseline_Customer_Results_df['Variable_Cost_per_t_Baseline']
    NO_Baseline_Customer_Results_df['Gross_Margin_Baseline'] = NO_Baseline_Customer_Results_df['Sales_Volume_Baseline'] * NO_Baseline_Customer_Results_df['Gross_Margin_per_t_Baseline']
    NO_Baseline_Customer_Results_df['Net_Revenue_Baseline'] = NO_Baseline_Customer_Results_df['Sales_Volume_Baseline'] * NO_Baseline_Customer_Results_df['Sales_Price']


    # Specified column order
    columns_order = [
        'Customers', 'Product', 'Incoterms', 'Sales_Price', 'Gross_Revenue_per_t',

        'Sales_Volume_Baseline', 'Quarries_Baseline', 'Gross_Revenue_Baseline',
        'Transport_Cost_per_t_Baseline', 'Transport_Cost_Baseline', 'Net_Revenue_Baseline',
        'Variable_Cost_per_t_Baseline', 'Variable_Cost_Sales_Baseline',
        'Gross_Margin_per_t_Baseline', 'Gross_Margin_Baseline'
    ]

    # Reindex the DataFrame with the specified column order
    NO_Baseline_Customer_Results_df = NO_Baseline_Customer_Results_df.reindex(columns=columns_order)


    # Calculate Total Gross Revenue
    NO_Baseline_Gross_Sales = NO_Baseline_Customer_Results_df['Gross_Revenue_Baseline'].sum(skipna=True)

    return NO_Baseline_Customer_Results_df, NO_Baseline_Gross_Sales



def Extract_Baseline_Results(project_data, NO_Baseline_Gross_Sales):
    # Retrieve dataframes
    NO_Baseline_Raw_Results_df_json = project_data.NO_Baseline_Raw_Results_df_json
    NO_Baseline_Raw_Results_df = pd.read_json(StringIO(NO_Baseline_Raw_Results_df_json), orient='records')
    baseline_infoTable_df_json = project_data.baseline_infoTable_df_json
    baseline_infoTable_df = pd.read_json(StringIO(baseline_infoTable_df_json), orient='records')


    # Extract and Calculate P&L Data
    # Extract Total Sales Volumes
    temp_sales_volume_df = NO_Baseline_Raw_Results_df[NO_Baseline_Raw_Results_df['Variable'] == 'Sales'].copy()
    temp_sales_volume_df['Value'] = temp_sales_volume_df['Value'].astype(float).round(1)
    NO_Baseline_Total_Sales = temp_sales_volume_df['Value'].sum()

    # Extract Inventory Change
    temp_inventory_df = NO_Baseline_Raw_Results_df[NO_Baseline_Raw_Results_df['Variable'] == 'Stocks_variation'].copy()
    temp_inventory_df['Value'] = temp_inventory_df['Value'].astype(float).round(1)
    NO_Baseline_Inventory_Change = temp_inventory_df['Value'].sum()

    # Calculate Total Production Volumes
    NO_Baseline_Total_Production = NO_Baseline_Total_Sales + NO_Baseline_Inventory_Change

    # Extract Distribution Cost
    temp_distribution_cost_df = NO_Baseline_Raw_Results_df[NO_Baseline_Raw_Results_df['Variable'] == 'Total_Distribution_cost'].copy()
    NO_Baseline_Distribution_Cost = temp_distribution_cost_df['Value']
    NO_Baseline_Distribution_Cost = int(NO_Baseline_Distribution_Cost.iloc[0])

    # Calculate Net Sales
    NO_Baseline_Net_Sales = NO_Baseline_Gross_Sales - NO_Baseline_Distribution_Cost

    # Extract Variable Cost
    temp_variable_cost_df = NO_Baseline_Raw_Results_df[NO_Baseline_Raw_Results_df['Variable'] == 'Total_Variable_cost'].copy()
    NO_Baseline_Variable_Cost = temp_variable_cost_df['Value']
    NO_Baseline_Variable_Cost = int(NO_Baseline_Variable_Cost.iloc[0])

    # Calculate Gross Margin
    NO_Baseline_Gross_Margin = NO_Baseline_Net_Sales - NO_Baseline_Variable_Cost

    # Extract Fixed Cost
    baseline_infoTable_df['Fixed_Cost'] = baseline_infoTable_df['Fixed_Cost'].astype(int)
    NO_Baseline_Fixed_Cost = baseline_infoTable_df['Fixed_Cost'].sum()

    # Calculate EBIT
    NO_Baseline_EBIT = NO_Baseline_Gross_Margin - NO_Baseline_Fixed_Cost

    # Calculate Gross Margin % and EBIT % on Gross Sales
    NO_Baseline_Gross_Margin_Percentage = (NO_Baseline_Gross_Margin / NO_Baseline_Gross_Sales) * 100
    NO_Baseline_Gross_Margin_Percentage = round(NO_Baseline_Gross_Margin_Percentage, 2)
    NO_Baseline_EBIT_Percentage = (NO_Baseline_EBIT / NO_Baseline_Gross_Sales) * 100
    NO_Baseline_EBIT_Percentage = round(NO_Baseline_EBIT_Percentage, 2)



    # Extract Opening, Operating Hours, Sales Volume and Stock Variation
    # Extract Opening Hours
    temp_opening_hours_df = NO_Baseline_Raw_Results_df[NO_Baseline_Raw_Results_df['Variable'] == 'Opening_hours'].copy()

    # Keep relevant columns
    temp_opening_hours_df = temp_opening_hours_df[['Variable', 'index1', 'Value']]
    temp_opening_hours_df['Value'] = temp_opening_hours_df['Value'].astype(float).round(1)

    # Grouping by specified columns and aggregating
    temp_opening_hours_df = temp_opening_hours_df.groupby(
        ['Variable', 'index1']).agg({
        'Value': 'sum',
    }).reset_index()

    # Drop Variable columns
    temp_opening_hours_df.drop(columns='Variable', inplace=True)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'Value': 'Opening_Hours'
    }
    temp_opening_hours_df.rename(columns=columns_to_rename, inplace=True)

    # Delete ' '
    temp_opening_hours_df['Quarries'] = temp_opening_hours_df['Quarries'].replace({"'": ""}, regex=True)



    # Extract Operating Hours
    temp_operating_hours_df = NO_Baseline_Raw_Results_df[NO_Baseline_Raw_Results_df['Variable'] == 'Operating_hours'].copy()

    # Keep relevant columns
    temp_operating_hours_df = temp_operating_hours_df[['Variable', 'index1', 'Value']]
    temp_operating_hours_df['Value'] = temp_operating_hours_df['Value'].astype(float).round(1)

    # Grouping by specified columns and aggregating
    temp_operating_hours_df = temp_operating_hours_df.groupby(
        ['Variable', 'index1']).agg({
        'Value': 'sum',
    }).reset_index()

    # Drop Variable columns
    temp_operating_hours_df.drop(columns='Variable', inplace=True)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'Value': 'Operating_Hours'
    }
    temp_operating_hours_df.rename(columns=columns_to_rename, inplace=True)

    # Delete ' '
    temp_operating_hours_df['Quarries'] = temp_operating_hours_df['Quarries'].replace({"'": ""}, regex=True)



    # Extract Sales Volumes
    temp_sales_volumes_df = NO_Baseline_Raw_Results_df[NO_Baseline_Raw_Results_df['Variable'] == 'Sales'].copy()

    # Keep relevant columns
    temp_sales_volumes_df = temp_sales_volumes_df[['Variable', 'index1', 'Value']]

    # Grouping by specified columns and aggregating
    temp_sales_volumes_df = temp_sales_volumes_df.groupby(
        ['Variable', 'index1']).agg({
        'Value': 'sum',
    }).reset_index()

    # Drop Variable columns
    temp_sales_volumes_df.drop(columns='Variable', inplace=True)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'Value': 'Sales_Volume'
    }
    temp_sales_volumes_df.rename(columns=columns_to_rename, inplace=True)

    # Delete ' '
    temp_sales_volumes_df['Quarries'] = temp_sales_volumes_df['Quarries'].replace({"'": ""}, regex=True)



    # Extract Stocks Variation
    temp_stock_variation_df = NO_Baseline_Raw_Results_df[NO_Baseline_Raw_Results_df['Variable'] == 'Stocks_variation'].copy()

    # Keep relevant columns
    temp_stock_variation_df = temp_stock_variation_df[['Variable', 'index1', 'Value']]

    # Grouping by specified columns and aggregating
    temp_stock_variation_df = temp_stock_variation_df.groupby(
        ['Variable', 'index1']).agg({
        'Value': 'sum',
    }).reset_index()

    # Drop Variable columns
    temp_stock_variation_df.drop(columns='Variable', inplace=True)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'Value': 'Stocks_Variation'
    }
    temp_stock_variation_df.rename(columns=columns_to_rename, inplace=True)

    # Delete ' '
    temp_stock_variation_df['Quarries'] = temp_stock_variation_df['Quarries'].replace({"'": ""}, regex=True)


    # Merge dataframes and Format
    NO_Baseline_Outlook_df = pd.merge(temp_opening_hours_df, temp_operating_hours_df, on=['Quarries'], how='left')
    NO_Baseline_Outlook_df = pd.merge(NO_Baseline_Outlook_df, temp_sales_volumes_df, on=['Quarries'], how='left')
    NO_Baseline_Outlook_df = pd.merge(NO_Baseline_Outlook_df, temp_stock_variation_df, on=['Quarries'], how='left')

    NO_Baseline_Outlook_df['Opening_Hours'] = NO_Baseline_Outlook_df['Opening_Hours'].astype(float).round(1)
    NO_Baseline_Outlook_df['Operating_Hours'] = NO_Baseline_Outlook_df['Operating_Hours'].astype(float).round(1)
    NO_Baseline_Outlook_df['Sales_Volume'] = NO_Baseline_Outlook_df['Sales_Volume'].astype(int)
    NO_Baseline_Outlook_df['Stocks_Variation'] = NO_Baseline_Outlook_df['Stocks_Variation'].astype(int)
    NO_Baseline_Outlook_df.sort_values(by='Quarries', inplace=True)


    # Format Values
    NO_Baseline_Outlook_df['Sales_Volume'] = NO_Baseline_Outlook_df['Sales_Volume'].apply(format_with_commas)
    NO_Baseline_Outlook_df['Stocks_Variation'] = NO_Baseline_Outlook_df['Stocks_Variation'].apply(format_with_commas)

    Baseline_PL_Items = {
        'NO_Baseline_Total_Sales': NO_Baseline_Total_Sales,
        'NO_Baseline_Inventory_Change': NO_Baseline_Inventory_Change,
        'NO_Baseline_Total_Production': NO_Baseline_Total_Production,
        'NO_Baseline_Gross_Sales': NO_Baseline_Gross_Sales,
        'NO_Baseline_Distribution_Cost': NO_Baseline_Distribution_Cost,
        'NO_Baseline_Net_Sales': NO_Baseline_Net_Sales,
        'NO_Baseline_Variable_Cost': NO_Baseline_Variable_Cost,
        'NO_Baseline_Gross_Margin': NO_Baseline_Gross_Margin,
        'NO_Baseline_Gross_Margin_Percentage': NO_Baseline_Gross_Margin_Percentage,
        'NO_Baseline_Fixed_Cost': NO_Baseline_Fixed_Cost,
        'NO_Baseline_EBIT': NO_Baseline_EBIT,
        'NO_Baseline_EBIT_Percentage': NO_Baseline_EBIT_Percentage,
    }

    return (NO_Baseline_Raw_Results_df, Baseline_PL_Items, NO_Baseline_Outlook_df)




def Create_Plotly_Visualization(project_data, NO_Baseline_Customer_Results_df) :
    # Keep relevant columns
    NO_Baseline_Customer_Results_df = NO_Baseline_Customer_Results_df[['Customers', 'Net_Revenue_Baseline', 'Sales_Volume_Baseline', 'Gross_Margin_Baseline']]

    # Grouping by specified columns and aggregating
    NO_Baseline_Customer_Results_df = NO_Baseline_Customer_Results_df.groupby(
        ['Customers']).agg({
        'Sales_Volume_Baseline': 'sum',
        'Net_Revenue_Baseline': 'sum',
        'Gross_Margin_Baseline': 'sum'
    }).reset_index()

    NO_Baseline_Customer_Results_df['Sales_Volume_Baseline'] = NO_Baseline_Customer_Results_df['Sales_Volume_Baseline'].astype(int)
    NO_Baseline_Customer_Results_df['Net_Revenue_Baseline'] = NO_Baseline_Customer_Results_df['Net_Revenue_Baseline'].astype(int)
    NO_Baseline_Customer_Results_df['Gross_Margin_Baseline'] = NO_Baseline_Customer_Results_df['Gross_Margin_Baseline'].astype(int)

    currency = project_data.currency



    # Create interactive scatter plot Sales Price
    fig = px.scatter(NO_Baseline_Customer_Results_df, x='Sales_Volume_Baseline', y='Net_Revenue_Baseline', hover_name='Customers')

    fig.update_traces(
        hovertemplate='<b style="font-family: Segoe UI;">Customers:</b> <span style="font-family: Segoe UI;">%{hovertext}</span><br>'
                      '<b style="font-family: Segoe UI;">Sales Volume:</b> <span style="font-family: Segoe UI;">%{x} t</span><br>'
                      '<b style="font-family: Segoe UI;">Revenue:</b> <span style="font-family: Segoe UI;">%{y} ' + currency + '</span><br><extra></extra>',

        marker=dict(
            color='#172b4d',  # Set point color to blue
            size=5  # Set point size
        )
    )

    # Update layout to change axis names, font, and set x-axis range
    fig.update_layout(
        title=dict(
            text="Net Sales Revenue Visualization",  # Specify chart title
            font=dict(
                family='Segoe UI',  # Specify font family
                size=24,  # Specify font size
                color='#172b4d',  # Specify font color
            ),
            x=0.5,  # Set x position to center (0 to 1, where 0 is left and 1 is right)
            y=0.95  # Set y position to be slightly above the plot area (0 to 1, where 0 is bottom and 1 is top)
        ),
        xaxis=dict(
            tickfont=dict(
                family='Segoe UI',  # Specify font family
                size=14,  # Specify font size
                color='#172b4d'  # Specify font color
            ),
            title=dict(
                text='Sales Volume [t]',  # Specify axis title text
                font=dict(
                    family='Segoe UI',  # Specify axis title font family
                    size=18,  # Specify axis title font size
                    color='#172b4d'  # Specify axis title font color
                )
            ),
            range=[0, round(NO_Baseline_Customer_Results_df['Sales_Volume_Baseline'].max() + 5000)],  # Set y-axis range to start from 0
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',  # Specify color of zero axis line

        ),
        yaxis=dict(
            tickfont=dict(
                family='Segoe UI',  # Specify font family
                size=14,  # Specify font size
                color='#172b4d'  # Specify font color
            ),
            title=dict(
                text=f'Revenue [{currency}]',  # Specify axis title text
                font=dict(
                    family='Segoe UI',  # Specify axis title font family
                    size=18,  # Specify axis title font size
                    color='#172b4d'  # Specify axis title font color
                )
            ),
            range=[round(NO_Baseline_Customer_Results_df['Net_Revenue_Baseline'].min() - 1),
                   round(NO_Baseline_Customer_Results_df['Net_Revenue_Baseline'].max() + 50000)],  # Set y-axis range to start from 0
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',
        ),
        plot_bgcolor='#f6f9fc',
        paper_bgcolor='#f6f9fc',  # Set background color around the chart
    )

    # Convert Plotly figure to HTML
    Plotly_Revenue = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})




    # Create interactive scatter plot GM
    fig = px.scatter(NO_Baseline_Customer_Results_df, x='Sales_Volume_Baseline', y='Gross_Margin_Baseline', hover_name='Customers')

    # Add custom hover information and set marker properties
    marker_color = ['#dc3545' if gm < 0 else '#172b4d' for gm in NO_Baseline_Customer_Results_df['Gross_Margin_Baseline']]

    fig.update_traces(
        hovertemplate='<b style="font-family: Segoe UI;">Customers:</b> <span style="font-family: Segoe UI;">%{hovertext}</span><br>'
                      '<b style="font-family: Segoe UI;">Sales Volume:</b> <span style="font-family: Segoe UI;">%{x} t</span><br>'
                      '<b style="font-family: Segoe UI;">Gross Margin:</b> <span style="font-family: Segoe UI;">%{y} ' + currency + '</span><br><extra></extra>',

        marker=dict(
            color=marker_color,  # Set point color to blue
            size=5  # Set point size
        )
    )

    # Update layout to change axis names, font, and set x-axis range
    fig.update_layout(
        title=dict(
            text="Gross Margin Visualization",  # Specify chart title
            font=dict(
                family='Segoe UI',  # Specify font family
                size=24,  # Specify font size
                color='#172b4d'  # Specify font color
            ),
            x=0.5,  # Set x position to center (0 to 1, where 0 is left and 1 is right)
            y=0.95  # Set y position to be slightly above the plot area (0 to 1, where 0 is bottom and 1 is top)
        ),
        xaxis=dict(
            tickfont=dict(
                family='Segoe UI',  # Specify font family
                size=14,  # Specify font size
                color='#172b4d'  # Specify font color
            ),
            title=dict(
                text='Sales Volume [t]',  # Specify axis title text
                font=dict(
                    family='Segoe UI',  # Specify axis title font family
                    size=18,  # Specify axis title font size
                    color='#172b4d'  # Specify axis title font color
                )
            ),
            range=[0, round(NO_Baseline_Customer_Results_df['Sales_Volume_Baseline'].max() + 5000)],  # Set y-axis range to start from 0
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',  # Specify color of zero axis line
        ),
        yaxis=dict(
            tickfont=dict(
                family='Segoe UI',  # Specify font family
                size=14,  # Specify font size
                color='#172b4d'  # Specify font color
            ),
            title=dict(
                text=f'Gross Margin [{currency}]',  # Specify axis title text
                font=dict(
                    family='Segoe UI',  # Specify axis title font family
                    size=18,  # Specify axis title font size
                    color='#172b4d'  # Specify axis title font color
                )
            ),
            range=[round(NO_Baseline_Customer_Results_df['Gross_Margin_Baseline'].min() - 50000),
                   round(NO_Baseline_Customer_Results_df['Gross_Margin_Baseline'].max() + 50000)],  # Set y-axis range to start from 0
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',  # Specify color of zero axis line
        ),
        plot_bgcolor='#f6f9fc',
        paper_bgcolor='#f6f9fc',  # Set background color around the chart
    )
    # Convert Plotly figure to HTML
    Plotly_Gross_Margin = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})


    return Plotly_Revenue, Plotly_Gross_Margin








def Extract_Customer_Results_Scenario(project_data, scenario):
    # Extract Customer Transactions
    # Retrieve dataframes
    NO_Scenario_Raw_Results_df_json = scenario.NO_Scenario_Raw_Results_df_json
    NO_Scenario_Raw_Results_df = pd.read_json(StringIO(NO_Scenario_Raw_Results_df_json), orient='records')

    # Extract Sales Details per Customer
    Customer_Sales_df = NO_Scenario_Raw_Results_df[NO_Scenario_Raw_Results_df['Variable'] == 'Sales'].copy()

    # Format Value
    Customer_Sales_df['Value'] = Customer_Sales_df['Value'].astype(float)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'index2': 'Customers',
        'index3': 'Product',
        'index4': 'Incoterms',
        'Value': 'Sales_Volume'
    }
    Customer_Sales_df.rename(columns=columns_to_rename, inplace=True)

    # Drop Variable columns
    Customer_Sales_df.drop(columns='Variable', inplace=True)

    # Delete ' '
    Customer_Sales_df['Quarries'] = Customer_Sales_df['Quarries'].replace({"'": ""}, regex=True)
    Customer_Sales_df['Customers'] = Customer_Sales_df['Customers'].replace({"'": ""}, regex=True)
    Customer_Sales_df['Product'] = Customer_Sales_df['Product'].replace({"'": ""}, regex=True)

    # Remove rows where Sales_Volume is 0
    Customer_Sales_df = Customer_Sales_df[Customer_Sales_df['Sales_Volume'] != 0].reset_index(drop=True)



    # Extract Sales Price per Customer
    # Retrieve dataframes
    reduced_customers_df_json = project_data.reduced_customers_df_json
    reduced_customers_df = pd.read_json(StringIO(reduced_customers_df_json), orient='records')

    # Keep relevant columns
    reduced_customers_df = reduced_customers_df[['Customers', 'Product', 'Incoterms', 'Revenue', 'Sales_Volume']]
    # Delete ' '
    reduced_customers_df['Customers'] = reduced_customers_df['Customers'].replace({"'": ""}, regex=True)
    reduced_customers_df['Product'] = reduced_customers_df['Product'].replace({"'": ""}, regex=True)

    # Grouping by specified columns and aggregating
    reduced_customers_df = reduced_customers_df.groupby(
        ['Customers', 'Product', 'Incoterms']).agg({
        'Revenue': 'sum',
        'Sales_Volume': 'sum',
    }).reset_index()

    reduced_customers_df['ASP'] = reduced_customers_df['Revenue'] / reduced_customers_df['Sales_Volume']
    reduced_customers_df = reduced_customers_df.drop(columns=['Revenue', 'Sales_Volume'])

    # Merge Customer_Sales_df and reduced_customers_df
    NO_Scenario_Customer_Results_df = pd.merge(Customer_Sales_df, reduced_customers_df, on=['Customers', 'Product', 'Incoterms'], how='left')



    # Extract Product Variable Cost per Quarry
    # Retrieve all related Quarry instances for the specific ProjectData
    quarries = project_data.quarries.all()

    # Initialize an empty list to store individual DataFrames
    dfs = []

    # Iterate through each quarry
    for quarry in quarries:
        # Read JSON data into a DataFrame
        P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
        P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')

        # Add 'Quarries' column with quarry name
        P4PFullSummary_df.insert(0, 'Quarries', quarry.name)

        # Append DataFrame to the list
        dfs.append(P4PFullSummary_df)

    # Concatenate all DataFrames in the list vertically
    Variable_Cost_df = pd.concat(dfs, ignore_index=True)

    # Calculate Variable Cost
    Variable_Cost_df['Variable_Cost'] = Variable_Cost_df['ASP'] - Variable_Cost_df['Agg_levy'] - Variable_Cost_df['GM']

    # Keep relevant columns
    Variable_Cost_df = Variable_Cost_df[['Quarries', 'Product', 'Variable_Cost']]

    # Retrieve unique quarries and products
    unique_quarries = Variable_Cost_df['Quarries'].unique()
    unique_products = Variable_Cost_df['Product'].unique()

    # Create a DataFrame with all combinations of quarries and products
    all_combinations = pd.DataFrame(list(product(unique_quarries, unique_products)), columns=['Quarries', 'Product'])

    # Merge with the original DataFrame to get Variable_Cost values
    Variable_Cost_df = pd.merge(all_combinations, Variable_Cost_df, on=['Quarries', 'Product'], how='left')

    # Fill missing Variable_Cost with 1000
    Variable_Cost_df['Variable_Cost'] = Variable_Cost_df['Variable_Cost'].fillna(1000)



    # Extract Transport_Cost_per_Customer
    # Retrieve dataframes
    Transport_Cost_per_Customer_df_json = project_data.Transport_Cost_per_Customer_df_json
    Transport_Cost_per_Customer_df = pd.read_json(StringIO(Transport_Cost_per_Customer_df_json), orient='records')

    # Delete ' '
    Transport_Cost_per_Customer_df['Quarries'] = Transport_Cost_per_Customer_df['Quarries'].replace({"'": ""}, regex=True)
    Transport_Cost_per_Customer_df['Customers'] = Transport_Cost_per_Customer_df['Customers'].replace({"'": ""}, regex=True)

    # Keep relevant columns
    Transport_Cost_per_Customer_df = Transport_Cost_per_Customer_df[['Quarries', 'Customers', 'Transport_Cost']]



    # Merge all Dataframes together
    # Merge for Variable Cost
    NO_Scenario_Customer_Results_df = pd.merge(NO_Scenario_Customer_Results_df, Variable_Cost_df, on=['Quarries', 'Product'], how='left')
    # Merge for Transport cost
    NO_Scenario_Customer_Results_df = pd.merge(NO_Scenario_Customer_Results_df, Transport_Cost_per_Customer_df, on=['Quarries', 'Customers'], how='left')

    # Transport = 0 if Incoterms = Ex-works
    # Define a function to apply the required logic
    def modify_transport_cost(row):
        if row['Incoterms'] == 'Ex-works':
            return 0
        return row['Transport_Cost']

    # Apply the function to the 'Transport_Cost' column
    NO_Scenario_Customer_Results_df['Transport_Cost'] = NO_Scenario_Customer_Results_df.apply(modify_transport_cost, axis=1)

    # Rename multiple columns
    columns_to_rename = {
        'Quarries': 'Quarries_Scenario',
        'Sales_Volume': 'Sales_Volume_Scenario',
        'Variable_Cost': 'Variable_Cost_per_t_Scenario',
        'Transport_Cost': 'Transport_Cost_per_t_Scenario'
    }
    NO_Scenario_Customer_Results_df.rename(columns=columns_to_rename, inplace=True)



    # Retrieve NO_Baseline_Customer_Results for Gross Revenue reference
    NO_Baseline_Customer_Results_df_json = project_data.NO_Baseline_Customer_Results_df_json
    NO_Baseline_Customer_Results_df = pd.read_json(StringIO(NO_Baseline_Customer_Results_df_json), orient='records')

   # Calculate Gross Revenue per ton for triplets Customers, Product, Incoterms
    temp_df = NO_Baseline_Customer_Results_df[['Customers', 'Product', 'Incoterms', 'Sales_Volume_Baseline', 'Gross_Revenue_Baseline']]

    temp_df = temp_df.groupby(
        ['Customers', 'Product', 'Incoterms']).agg({
        'Sales_Volume_Baseline': 'sum',
        'Gross_Revenue_Baseline': 'sum'
    }).reset_index()

    # Calculate Gross_Revenue_per_t
    temp_df['Gross_Revenue_per_t'] = temp_df['Gross_Revenue_Baseline'] / temp_df['Sales_Volume_Baseline']

    # Keep relevant columns
    temp_df = temp_df[['Customers', 'Product', 'Incoterms', 'Gross_Revenue_per_t']]

    # Merge dataframes to have Gross_Revenue_per_t for NO_Scenario_Customer_Results_df
    NO_Scenario_Customer_Results_df = pd.merge(NO_Scenario_Customer_Results_df, temp_df, on=['Customers', 'Product', 'Incoterms'], how='left')

    # Calculate Gross_Revenue_Scenario
    NO_Scenario_Customer_Results_df['Gross_Revenue_Scenario'] = NO_Scenario_Customer_Results_df['Gross_Revenue_per_t'] * NO_Scenario_Customer_Results_df['Sales_Volume_Scenario']

    # Calculate Total Gross Revenue
    NO_Scenario_Gross_Sales = NO_Scenario_Customer_Results_df['Gross_Revenue_Scenario'].sum(skipna=True)

    # Renaming 'Gross_Revenue' column to 'Gross_Revenue_Scenario'
    NO_Scenario_Customer_Results_df.rename(columns={'ASP': 'Sales_Price'}, inplace=True)

    # Calculation
    NO_Scenario_Customer_Results_df['Transport_Cost_Scenario'] = NO_Scenario_Customer_Results_df['Sales_Volume_Scenario'] * NO_Scenario_Customer_Results_df['Transport_Cost_per_t_Scenario']
    NO_Scenario_Customer_Results_df['Variable_Cost_Sales_Scenario'] = NO_Scenario_Customer_Results_df['Sales_Volume_Scenario'] * NO_Scenario_Customer_Results_df['Variable_Cost_per_t_Scenario']
    NO_Scenario_Customer_Results_df['Gross_Margin_per_t_Scenario'] = NO_Scenario_Customer_Results_df['Gross_Revenue_per_t'] - NO_Scenario_Customer_Results_df['Transport_Cost_per_t_Scenario'] - NO_Scenario_Customer_Results_df['Variable_Cost_per_t_Scenario']
    NO_Scenario_Customer_Results_df['Gross_Margin_Scenario'] = NO_Scenario_Customer_Results_df['Sales_Volume_Scenario'] * NO_Scenario_Customer_Results_df['Gross_Margin_per_t_Scenario']
    NO_Scenario_Customer_Results_df['Net_Revenue_Scenario'] = NO_Scenario_Customer_Results_df['Sales_Volume_Scenario'] * NO_Scenario_Customer_Results_df['Sales_Price']

    # Specified column order
    columns_order = [
        'Customers', 'Product', 'Incoterms', 'Sales_Price', 'Gross_Revenue_per_t',

        'Sales_Volume_Scenario', 'Quarries_Scenario', 'Gross_Revenue_Scenario',
        'Transport_Cost_per_t_Scenario', 'Transport_Cost_Scenario', 'Net_Revenue_Scenario',
        'Variable_Cost_per_t_Scenario', 'Variable_Cost_Sales_Scenario',
        'Gross_Margin_per_t_Scenario', 'Gross_Margin_Scenario'
    ]

    # Reindex the DataFrame with the specified column order
    NO_Scenario_Customer_Results_df = NO_Scenario_Customer_Results_df.reindex(columns=columns_order)


    return NO_Scenario_Customer_Results_df, NO_Scenario_Gross_Sales



def Waterfall_Chart_Delta_Scenario(project_data, Delta_PL_Items, Customer_Delta_Baseline_Scenario_df):
    currency = project_data.currency
    # Extract relevant data for waterfall chart
    NO_Delta_Gross_Sales = Delta_PL_Items['NO_Delta_Gross_Sales']
    NO_Delta_Distribution_Cost = -Delta_PL_Items['NO_Delta_Distribution_Cost']
    NO_Delta_Variable_Cost = -Delta_PL_Items['NO_Delta_Variable_Cost']
    NO_Delta_Fixed_Cost = -Delta_PL_Items['NO_Delta_Fixed_Cost']

    Customer_Delta_Baseline_Scenario_df['Delta_Variable_Cost_Sales'] = Customer_Delta_Baseline_Scenario_df['Variable_Cost_Sales_Scenario'] - Customer_Delta_Baseline_Scenario_df['Variable_Cost_Sales_Baseline']
    NO_Delta_Variable_Cost_Sales = -int(Customer_Delta_Baseline_Scenario_df['Delta_Variable_Cost_Sales'].sum())

    NO_Delta_Variable_Cost_Stock = NO_Delta_Variable_Cost - NO_Delta_Variable_Cost_Sales

    # Create waterfall chart
    fig = go.Figure(go.Waterfall(
        name="",
        orientation="v",
        measure=["relative", "relative", "relative", "relative", "relative", "total"],
        x=["Gross Sales", "Distribution Cost", "Variable Cost Stock", "Variable Cost Sales", "Fixed Cost", "Total"],
        y=[NO_Delta_Gross_Sales, NO_Delta_Distribution_Cost, NO_Delta_Variable_Cost_Stock,
           NO_Delta_Variable_Cost_Sales, NO_Delta_Fixed_Cost, 0],
        increasing={
            'marker': {'color': '#2dce89'},
        },
        decreasing={
            'marker': {'color': '#f5365c'},
        },
        totals={
            'marker': {'color': '#172b4d'},
        },
        connector={"line": {"color": "rgb(63, 63, 63)"}},
    ))

    fig.update_layout(
        title="P&L Impact Breakdown",
        title_font=dict(
            family='Segoe UI',  # Specify font family
            size=24,  # Specify font size
            color='#172b4d',  # Specify the font color
        ),
        yaxis=dict(
            title=f"Impact [{currency}]",
            titlefont=dict(
                family='Segoe UI',  # Specify axis title font family
                size=15,  # Specify axis title font size
                color='#172b4d'  # Specify axis title font color
            ),
            tickformat=',.0f',  # Format tick labels to remove decimal places
            tickfont=dict(
                family='Segoe UI',  # Specify axis title font family
                size=13,  # Specify axis title font size
                color='#172b4d'  # Specify the font color
            ),
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',
        ),
        xaxis=dict(
            tickfont=dict(
                family="Segoe UI",
                size=15,
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

    # Convert the figure to HTML
    Impact_Breakdown_Chart = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})

    return Impact_Breakdown_Chart



def Customer_Delta_Baseline_Scenario(project_data, NO_Scenario_Customer_Results_df):
    # Retrieve NO_Baseline_Customer_Results for Gross Revenue reference
    NO_Baseline_Customer_Results_df_json = project_data.NO_Baseline_Customer_Results_df_json
    NO_Baseline_Customer_Results_df = pd.read_json(StringIO(NO_Baseline_Customer_Results_df_json), orient='records')

    # Keep relevant columns
    NO_Baseline_Customer_Results_df = NO_Baseline_Customer_Results_df[['Customers', 'Gross_Revenue_Baseline', 'Sales_Volume_Baseline', 'Transport_Cost_Baseline', 'Variable_Cost_Sales_Baseline']]

    # Grouping by specified columns and aggregating
    NO_Baseline_Customer_Results_df = NO_Baseline_Customer_Results_df.groupby(
        ['Customers']).agg({
        'Gross_Revenue_Baseline': 'sum',
        'Sales_Volume_Baseline': 'sum',
        'Transport_Cost_Baseline': 'sum',
        'Variable_Cost_Sales_Baseline': 'sum'
    }).reset_index()


    # Keep relevant columns
    NO_Scenario_Customer_Results_df = NO_Scenario_Customer_Results_df[['Customers', 'Gross_Revenue_Scenario', 'Sales_Volume_Scenario', 'Transport_Cost_Scenario', 'Variable_Cost_Sales_Scenario']]

    # Grouping by specified columns and aggregating
    NO_Scenario_Customer_Results_df = NO_Scenario_Customer_Results_df.groupby(
        ['Customers']).agg({
        'Gross_Revenue_Scenario': 'sum',
        'Sales_Volume_Scenario': 'sum',
        'Transport_Cost_Scenario': 'sum',
        'Variable_Cost_Sales_Scenario': 'sum'
    }).reset_index()

    # Merge both Baseline and Scenario dataframes
    Customer_Delta_Baseline_Scenario_df = pd.merge(NO_Baseline_Customer_Results_df, NO_Scenario_Customer_Results_df, on=['Customers'], how='left')

    # Calculate total GM per customer
    Customer_Delta_Baseline_Scenario_df['Total_GM_Sales_Baseline'] = Customer_Delta_Baseline_Scenario_df['Gross_Revenue_Baseline'] - Customer_Delta_Baseline_Scenario_df['Transport_Cost_Baseline'] - Customer_Delta_Baseline_Scenario_df['Variable_Cost_Sales_Baseline']
    Customer_Delta_Baseline_Scenario_df['Total_GM_Sales_Scenario'] = Customer_Delta_Baseline_Scenario_df['Gross_Revenue_Scenario'] - Customer_Delta_Baseline_Scenario_df['Transport_Cost_Scenario'] - Customer_Delta_Baseline_Scenario_df['Variable_Cost_Sales_Scenario']

    # Calculate Delta GM
    Customer_Delta_Baseline_Scenario_df['Delta_GM_Sales_Baseline_Scenario'] = Customer_Delta_Baseline_Scenario_df['Total_GM_Sales_Scenario'] - Customer_Delta_Baseline_Scenario_df['Total_GM_Sales_Baseline']

    # Sorting the DataFrame by 'Delta_GM_Baseline_Scenario' from high to low
    Customer_Delta_Baseline_Scenario_df = Customer_Delta_Baseline_Scenario_df.sort_values(
        by='Delta_GM_Sales_Baseline_Scenario',
        ascending=False
    )

    # Keeping only the first 20 rows
    Customer_Delta_Baseline_Scenario_display = Customer_Delta_Baseline_Scenario_df.head(20).copy()

    # Format Dataframe
    Customer_Delta_Baseline_Scenario_display['Gross_Revenue_Baseline'] = Customer_Delta_Baseline_Scenario_display['Gross_Revenue_Baseline'].apply(format_with_commas)
    Customer_Delta_Baseline_Scenario_display['Sales_Volume_Baseline'] = Customer_Delta_Baseline_Scenario_display['Sales_Volume_Baseline'].apply(format_with_commas)
    Customer_Delta_Baseline_Scenario_display['Transport_Cost_Baseline'] = Customer_Delta_Baseline_Scenario_display['Transport_Cost_Baseline'].apply(format_with_commas)
    Customer_Delta_Baseline_Scenario_display['Variable_Cost_Sales_Baseline'] = Customer_Delta_Baseline_Scenario_display['Variable_Cost_Sales_Baseline'].apply(format_with_commas)
    Customer_Delta_Baseline_Scenario_display['Gross_Revenue_Scenario'] = Customer_Delta_Baseline_Scenario_display['Gross_Revenue_Scenario'].apply(format_with_commas)
    Customer_Delta_Baseline_Scenario_display['Sales_Volume_Scenario'] = Customer_Delta_Baseline_Scenario_display['Sales_Volume_Scenario'].apply(format_with_commas)
    Customer_Delta_Baseline_Scenario_display['Transport_Cost_Scenario'] = Customer_Delta_Baseline_Scenario_display['Transport_Cost_Scenario'].apply(format_with_commas)
    Customer_Delta_Baseline_Scenario_display['Variable_Cost_Sales_Scenario'] = Customer_Delta_Baseline_Scenario_display['Variable_Cost_Sales_Scenario'].apply(format_with_commas)
    Customer_Delta_Baseline_Scenario_display['Delta_GM_Sales_Baseline_Scenario'] = Customer_Delta_Baseline_Scenario_display['Delta_GM_Sales_Baseline_Scenario'].apply(format_with_commas)


    return Customer_Delta_Baseline_Scenario_df, Customer_Delta_Baseline_Scenario_display




def Extract_Scenario_Results(scenario, NO_Scenario_Gross_Sales):
    # Retrieve dataframes
    NO_Scenario_Raw_Results_df_json = scenario.NO_Scenario_Raw_Results_df_json
    NO_Scenario_Raw_Results_df = pd.read_json(StringIO(NO_Scenario_Raw_Results_df_json), orient='records')
    scenario_infoTable_df_json = scenario.scenario_infoTable_df_json
    scenario_infoTable_df = pd.read_json(StringIO(scenario_infoTable_df_json), orient='records')


    # Extract and Calculate P&L Data
    # Extract Total Sales Volumes
    temp_sales_volume_df = NO_Scenario_Raw_Results_df[NO_Scenario_Raw_Results_df['Variable'] == 'Sales'].copy()
    temp_sales_volume_df['Value'] = temp_sales_volume_df['Value'].astype(float).round(1)
    NO_Scenario_Total_Sales = temp_sales_volume_df['Value'].sum()

    # Extract Inventory Change
    temp_inventory_df = NO_Scenario_Raw_Results_df[NO_Scenario_Raw_Results_df['Variable'] == 'Stocks_variation'].copy()
    temp_inventory_df['Value'] = temp_inventory_df['Value'].astype(float).round(1)
    NO_Scenario_Inventory_Change = temp_inventory_df['Value'].sum()

    # Calculate Total Production Volumes
    NO_Scenario_Total_Production = NO_Scenario_Total_Sales + NO_Scenario_Inventory_Change

    # Extract Distribution Cost
    temp_distribution_cost_df = NO_Scenario_Raw_Results_df[NO_Scenario_Raw_Results_df['Variable'] == 'Total_Distribution_cost'].copy()
    NO_Scenario_Distribution_Cost = temp_distribution_cost_df['Value']
    NO_Scenario_Distribution_Cost = int(NO_Scenario_Distribution_Cost.iloc[0])

    # Calculate Net Sales
    NO_Scenario_Net_Sales = NO_Scenario_Gross_Sales - NO_Scenario_Distribution_Cost

    # Extract Variable Cost
    temp_variable_cost_df = NO_Scenario_Raw_Results_df[NO_Scenario_Raw_Results_df['Variable'] == 'Total_Variable_cost'].copy()
    NO_Scenario_Variable_Cost = temp_variable_cost_df['Value']
    NO_Scenario_Variable_Cost = int(NO_Scenario_Variable_Cost.iloc[0])

    # Calculate Gross Margin
    NO_Scenario_Gross_Margin = NO_Scenario_Net_Sales - NO_Scenario_Variable_Cost

    # Extract Fixed Cost
    scenario_infoTable_df['Fixed_Cost'] = scenario_infoTable_df['Fixed_Cost'].astype(int)
    NO_Scenario_Fixed_Cost = scenario_infoTable_df['Fixed_Cost'].sum()

    # Calculate EBIT
    NO_Scenario_EBIT = NO_Scenario_Gross_Margin - NO_Scenario_Fixed_Cost

    # Calculate Gross Margin % and EBIT % on Gross Sales
    NO_Scenario_Gross_Margin_Percentage = (NO_Scenario_Gross_Margin / NO_Scenario_Gross_Sales) * 100
    NO_Scenario_Gross_Margin_Percentage = round(NO_Scenario_Gross_Margin_Percentage, 2)
    NO_Scenario_EBIT_Percentage = (NO_Scenario_EBIT / NO_Scenario_Gross_Sales) * 100
    NO_Scenario_EBIT_Percentage = round(NO_Scenario_EBIT_Percentage, 2)



    # Extract Operating Hours, Sales Volume and Stock Variation

    # Extract Operating Hours
    temp_operating_hours_df = NO_Scenario_Raw_Results_df[NO_Scenario_Raw_Results_df['Variable'] == 'Operating_hours'].copy()

    # Keep relevant columns
    temp_operating_hours_df = temp_operating_hours_df[['Variable', 'index1', 'Value']]
    temp_operating_hours_df['Value'] = temp_operating_hours_df['Value'].astype(float).round(1)

    # Grouping by specified columns and aggregating
    temp_operating_hours_df = temp_operating_hours_df.groupby(
        ['Variable', 'index1']).agg({
        'Value': 'sum',
    }).reset_index()

    # Drop Variable columns
    temp_operating_hours_df.drop(columns='Variable', inplace=True)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'Value': 'Operating_Hours'
    }
    temp_operating_hours_df.rename(columns=columns_to_rename, inplace=True)

    # Delete ' '
    temp_operating_hours_df['Quarries'] = temp_operating_hours_df['Quarries'].replace({"'": ""}, regex=True)

    # Extract Opening Hours
    temp_opening_hours_df = NO_Scenario_Raw_Results_df[
        NO_Scenario_Raw_Results_df['Variable'] == 'Opening_hours'].copy()

    # Keep relevant columns
    temp_opening_hours_df = temp_opening_hours_df[['Variable', 'index1', 'Value']]
    temp_opening_hours_df['Value'] = temp_opening_hours_df['Value'].astype(float).round(1)

    # Grouping by specified columns and aggregating
    temp_opening_hours_df = temp_opening_hours_df.groupby(
        ['Variable', 'index1']).agg({
        'Value': 'sum',
    }).reset_index()

    # Drop Variable columns
    temp_opening_hours_df.drop(columns='Variable', inplace=True)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'Value': 'Opening_Hours'
    }
    temp_opening_hours_df.rename(columns=columns_to_rename, inplace=True)

    # Delete ' '
    temp_opening_hours_df['Quarries'] = temp_opening_hours_df['Quarries'].replace({"'": ""}, regex=True)
    temp_opening_hours_df['Opening_Hours'] = temp_opening_hours_df['Opening_Hours'].astype(float).round(1)


    # Extract Sales Volumes
    temp_sales_volumes_df = NO_Scenario_Raw_Results_df[NO_Scenario_Raw_Results_df['Variable'] == 'Sales'].copy()

    # Keep relevant columns
    temp_sales_volumes_df = temp_sales_volumes_df[['Variable', 'index1', 'Value']]

    # Grouping by specified columns and aggregating
    temp_sales_volumes_df = temp_sales_volumes_df.groupby(
        ['Variable', 'index1']).agg({
        'Value': 'sum',
    }).reset_index()

    # Drop Variable columns
    temp_sales_volumes_df.drop(columns='Variable', inplace=True)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'Value': 'Sales_Volume'
    }
    temp_sales_volumes_df.rename(columns=columns_to_rename, inplace=True)

    # Delete ' '
    temp_sales_volumes_df['Quarries'] = temp_sales_volumes_df['Quarries'].replace({"'": ""}, regex=True)



    # Extract Stocks Variation
    temp_stock_variation_df = NO_Scenario_Raw_Results_df[NO_Scenario_Raw_Results_df['Variable'] == 'Stocks_variation'].copy()

    # Keep relevant columns
    temp_stock_variation_df = temp_stock_variation_df[['Variable', 'index1', 'Value']]

    # Grouping by specified columns and aggregating
    temp_stock_variation_df = temp_stock_variation_df.groupby(
        ['Variable', 'index1']).agg({
        'Value': 'sum',
    }).reset_index()

    # Drop Variable columns
    temp_stock_variation_df.drop(columns='Variable', inplace=True)

    # Rename multiple columns
    columns_to_rename = {
        'index1': 'Quarries',
        'Value': 'Stocks_Variation'
    }
    temp_stock_variation_df.rename(columns=columns_to_rename, inplace=True)

    # Delete ' '
    temp_stock_variation_df['Quarries'] = temp_stock_variation_df['Quarries'].replace({"'": ""}, regex=True)


    # Merge dataframes and Format
    NO_Scenario_Outlook_df = pd.merge(temp_operating_hours_df, temp_sales_volumes_df, on=['Quarries'], how='left')
    NO_Scenario_Outlook_df = pd.merge(NO_Scenario_Outlook_df, temp_stock_variation_df, on=['Quarries'], how='left')
    NO_Scenario_Outlook_df = pd.merge(NO_Scenario_Outlook_df, temp_opening_hours_df, on=['Quarries'], how='left')

    NO_Scenario_Outlook_df['Operating_Hours'] = NO_Scenario_Outlook_df['Operating_Hours'].astype(float).round(1)
    NO_Scenario_Outlook_df['Sales_Volume'] = NO_Scenario_Outlook_df['Sales_Volume'].astype(int)
    NO_Scenario_Outlook_df['Stocks_Variation'] = NO_Scenario_Outlook_df['Stocks_Variation'].astype(int)
    NO_Scenario_Outlook_df.sort_values(by='Quarries', inplace=True)

    # Format Values
    NO_Scenario_Outlook_df['Sales_Volume'] = NO_Scenario_Outlook_df['Sales_Volume'].apply(format_with_commas)
    NO_Scenario_Outlook_df['Stocks_Variation'] = NO_Scenario_Outlook_df['Stocks_Variation'].apply(format_with_commas)

    Scenario_PL_Items = {
        'NO_Scenario_Total_Sales': NO_Scenario_Total_Sales,
        'NO_Scenario_Inventory_Change': NO_Scenario_Inventory_Change,
        'NO_Scenario_Total_Production': NO_Scenario_Total_Production,
        'NO_Scenario_Gross_Sales': NO_Scenario_Gross_Sales,
        'NO_Scenario_Distribution_Cost': NO_Scenario_Distribution_Cost,
        'NO_Scenario_Net_Sales': NO_Scenario_Net_Sales,
        'NO_Scenario_Variable_Cost': NO_Scenario_Variable_Cost,
        'NO_Scenario_Gross_Margin': NO_Scenario_Gross_Margin,
        'NO_Scenario_Gross_Margin_Percentage': NO_Scenario_Gross_Margin_Percentage,
        'NO_Scenario_Fixed_Cost': NO_Scenario_Fixed_Cost,
        'NO_Scenario_EBIT': NO_Scenario_EBIT,
        'NO_Scenario_EBIT_Percentage': NO_Scenario_EBIT_Percentage,
    }

    return (NO_Scenario_Raw_Results_df, Scenario_PL_Items, NO_Scenario_Outlook_df)







def Calculate_Baseline_Scenario_PL_Delta(Baseline_PL_Items, Scenario_PL_Items):
    # Delta P&L items
    NO_Delta_Total_Sales = Scenario_PL_Items['NO_Scenario_Total_Sales'] - Baseline_PL_Items['NO_Baseline_Total_Sales']
    NO_Delta_Inventory_Change = Scenario_PL_Items['NO_Scenario_Inventory_Change'] - Baseline_PL_Items['NO_Baseline_Inventory_Change']
    NO_Delta_Total_Production = Scenario_PL_Items['NO_Scenario_Total_Production'] - Baseline_PL_Items['NO_Baseline_Total_Production']
    NO_Delta_Gross_Sales = Scenario_PL_Items['NO_Scenario_Gross_Sales'] - Baseline_PL_Items['NO_Baseline_Gross_Sales']
    NO_Delta_Distribution_Cost = Scenario_PL_Items['NO_Scenario_Distribution_Cost'] - Baseline_PL_Items['NO_Baseline_Distribution_Cost']
    NO_Delta_Net_Sales = Scenario_PL_Items['NO_Scenario_Net_Sales'] - Baseline_PL_Items['NO_Baseline_Net_Sales']
    NO_Delta_Variable_Cost = Scenario_PL_Items['NO_Scenario_Variable_Cost'] - Baseline_PL_Items['NO_Baseline_Variable_Cost']
    NO_Delta_Gross_Margin = Scenario_PL_Items['NO_Scenario_Gross_Margin'] - Baseline_PL_Items['NO_Baseline_Gross_Margin']
    NO_Delta_Gross_Margin_Percentage = Scenario_PL_Items['NO_Scenario_Gross_Margin_Percentage'] - Baseline_PL_Items['NO_Baseline_Gross_Margin_Percentage']
    NO_Delta_Fixed_Cost = Scenario_PL_Items['NO_Scenario_Fixed_Cost'] - Baseline_PL_Items['NO_Baseline_Fixed_Cost']
    NO_Delta_EBIT = Scenario_PL_Items['NO_Scenario_EBIT'] - Baseline_PL_Items['NO_Baseline_EBIT']
    NO_Delta_EBIT_Percentage = Scenario_PL_Items['NO_Scenario_EBIT_Percentage'] - Baseline_PL_Items['NO_Baseline_EBIT_Percentage']

    # Format
    NO_Delta_Gross_Margin_Percentage = round(NO_Delta_Gross_Margin_Percentage, 2)
    NO_Delta_EBIT_Percentage = round(NO_Delta_EBIT_Percentage, 2)

    # Create dictionary
    Delta_PL_Items = {
        'NO_Delta_Total_Sales': NO_Delta_Total_Sales,
        'NO_Delta_Inventory_Change': NO_Delta_Inventory_Change,
        'NO_Delta_Total_Production': NO_Delta_Total_Production,
        'NO_Delta_Gross_Sales': NO_Delta_Gross_Sales,
        'NO_Delta_Distribution_Cost': NO_Delta_Distribution_Cost,
        'NO_Delta_Net_Sales': NO_Delta_Net_Sales,
        'NO_Delta_Variable_Cost': NO_Delta_Variable_Cost,
        'NO_Delta_Gross_Margin': NO_Delta_Gross_Margin,
        'NO_Delta_Gross_Margin_Percentage': NO_Delta_Gross_Margin_Percentage,
        'NO_Delta_Fixed_Cost': NO_Delta_Fixed_Cost,
        'NO_Delta_EBIT': NO_Delta_EBIT,
        'NO_Delta_EBIT_Percentage': NO_Delta_EBIT_Percentage,
    }

    return Delta_PL_Items


def Format_PL_Items_Dictionaries(PL_Dictionary):
    # Format PL_Dictionary
    for key, value in PL_Dictionary.items():
        if "Percentage" not in key:  # Skip percentages
            PL_Dictionary[key] = format_with_commas(value)

    return PL_Dictionary




def NO_Check_Quarries_Hours_Volumes_Capacity(project_data, Max_Opening_Hours_Max_Sales_Volume_df, reduced_customers_df):
    # Retrieve the list of quarries
    quarries = sorted(project_data.quarries.all(), key=lambda x: x.name)

    # Initialize an empty list to hold DataFrames
    temp_Throughput_Coeff_df = []

    # Iterate over the quarries
    for quarry in quarries:
        # Load the JSON data into a DataFrame
        capacityModes_df_json = quarry.capacityModes_df_json
        if capacityModes_df_json:  # Check if the JSON is not empty
            capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
            # Add a new column 'Quarry' with the name of the quarry
            capacityModes_df['Quarry'] = quarry.name
            temp_Throughput_Coeff_df.append(capacityModes_df)

    # Put everything together
    Throughput_Coeff_df = pd.concat(temp_Throughput_Coeff_df, ignore_index=True)

    # Keep relevant columns
    Throughput_Coeff_df = Throughput_Coeff_df[['Quarry', 'Modes', 'Max_capacity', 'Coefficient']]

    # Calculate Capacity
    Throughput_Coeff_df['Capacity'] = Throughput_Coeff_df['Max_capacity'] * Throughput_Coeff_df['Coefficient'] / 100

    # Get the smallest Capacity for each Quarry
    Min_Capacity_df = Throughput_Coeff_df.groupby('Quarry')['Capacity'].min().reset_index()

    # Rename the column for clarity
    Min_Capacity_df.rename(columns={'Capacity': 'Min_Capacity'}, inplace=True)

    # Create Enough_capacity_df
    Enough_capacity_df = pd.merge(Max_Opening_Hours_Max_Sales_Volume_df, Min_Capacity_df, on=['Quarry'], how='left')
    Enough_capacity_df ['Max_Operating_Volume'] = Enough_capacity_df['Max_Opening_Hours'] * Enough_capacity_df['Min_Capacity']
    Enough_capacity_df.rename(columns={'Quarry': 'Quarries'}, inplace=True)

    # Get the Sales_Volume per Quarry and Incoterm
    sales_volume_summary = reduced_customers_df.groupby(['Quarries', 'Incoterms'])['Sales_Volume'].sum().reset_index()
    ex_works_df = sales_volume_summary[sales_volume_summary['Incoterms'] == 'Ex-works'].copy()
    ex_works_df.rename(columns={'Sales_Volume': 'EXW_Sales_Volume'}, inplace=True)

    # Merge both dataframes
    Enough_capacity_df = pd.merge(Enough_capacity_df, ex_works_df, on=['Quarries'], how='left')
    Enough_capacity_df = Enough_capacity_df[['Quarries', 'EXW_Sales_Volume', 'Max_Operating_Volume', 'Max_Sales_Volume']]

    # Create 'Enough_Capacity' column
    Enough_capacity_df['Enough_Capacity'] = Enough_capacity_df.apply(
        lambda row: 0 if (row['Max_Operating_Volume'] < row['EXW_Sales_Volume'] or row['Max_Sales_Volume'] < row[
            'EXW_Sales_Volume']) else 1, axis=1)

    return Enough_capacity_df