import pandas as pd
from io import StringIO


def P4P_Completion(project_data):
    # Retrieve the list of quarries
    quarries_list = sorted(project_data.quarries.all(), key=lambda x: x.name)

    # Fetch data for each quarry
    P4P_results_data = {}
    clusterToBox_df_change_data = {}  # Initialize dictionary to store clusterToBox_df_change data
    for quarry in quarries_list:
        # Add sales forecast data to the dictionary with quarry as key
        P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
        P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
        P4P_results_data[quarry] = P4PFullSummary_df
        # Add clusterToBox_df_change to the dictionary with quarry as key
        clusterToBox_df_change = quarry.clusterToBox_df_change
        clusterToBox_df_change_data[quarry] = clusterToBox_df_change  # Store boolean data

    # Initialize lists to store quarry names, IDs, and conditions
    quarry_names = []
    quarry_ids = []
    conditions = []
    clusterToBox_change = []

    # Iterate through the quarries and check if the sales forecast data is empty
    for quarry in quarries_list:
        quarry_names.append(quarry.name)
        quarry_ids.append(quarry.quarry_id)
        conditions.append(P4P_results_data[quarry].empty)
        clusterToBox_change.append(clusterToBox_df_change_data[quarry])  # Retrieve boolean data

    # Create a DataFrame with quarry names, IDs, and conditions
    P4P_empty_df = pd.DataFrame({
        'Quarry': quarry_names,
        'Quarry_ID': quarry_ids,
        'Condition': conditions,
        'clusterToBox_change': clusterToBox_change,
    })

    # Count the number of unique quarries in the DataFrame
    num_quarries = P4P_empty_df['Quarry'].nunique()

    # Filter the DataFrame where both Condition and clusterToBox_change are False
    filtered_df = P4P_empty_df[(P4P_empty_df['Condition'] == False) & (P4P_empty_df['clusterToBox_change'] == False)]

    # Count the occurrences
    count = filtered_df.shape[0]

    # Calculate P4P_completion
    P4P_completion = ((count / num_quarries) * 100)

    return P4P_completion


def B4C_Completion(project_data):
    # Retrieve the list of quarries
    quarries_list = sorted(project_data.quarries.all(), key=lambda x: x.name)

    # Initiate data for each quarry
    B4C_results_data = {}
    P4P_results_data = {}
    clusterToBox_df_change_data = {}


    for quarry in quarries_list:
        # Add capacityModes_df to the dictionary with quarry as key
        capacityModes_df_json = quarry.capacityModes_df_json
        capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
        B4C_results_data[quarry] = capacityModes_df

        # Add P4PFullSummary_df to the dictionary with quarry as key
        P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
        P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
        P4P_results_data[quarry] = P4PFullSummary_df

        # Add clusterToBox_df_change to the dictionary with quarry as key
        clusterToBox_df_change = quarry.clusterToBox_df_change
        clusterToBox_df_change_data[quarry] = clusterToBox_df_change  # Store boolean data

    # Initialize lists to store quarry names, IDs, and conditions
    quarry_names = []
    quarry_ids = []
    conditions_B4C = []
    conditions_P4P = []
    clusterToBox_change = []


    # Iterate through the quarries and check if the sales forecast data is empty
    for quarry in quarries_list:
        quarry_names.append(quarry.name)
        quarry_ids.append(quarry.quarry_id)
        conditions_B4C.append(B4C_results_data[quarry].empty)
        conditions_P4P.append(P4P_results_data[quarry].empty)
        clusterToBox_change.append(clusterToBox_df_change_data[quarry])  # Retrieve boolean data


    # Create a DataFrame with quarry names, IDs, and conditions
    B4C_empty_df = pd.DataFrame({
        'Quarry': quarry_names,
        'Quarry_ID': quarry_ids,
        'Condition_B4C': conditions_B4C,
        'Condition_P4P': conditions_P4P,
        'clusterToBox_change': clusterToBox_change,
    })

    # Count the number of unique quarries in the DataFrame
    num_quarries = B4C_empty_df['Quarry'].nunique()

    # Filter the DataFrame where both Condition and clusterToBox_change are False
    filtered_df = B4C_empty_df[(B4C_empty_df['Condition_B4C'] == False) & (B4C_empty_df['Condition_P4P'] == False) & (B4C_empty_df['clusterToBox_change'] == False)]

    # Count the occurrences
    count = filtered_df.shape[0]

    # Calculate P4P_completion
    B4C_Completion = ((count / num_quarries) * 100)

    return B4C_Completion


def FarSeer_Completion(project_data):
    # Retrieve the list of quarries
    quarries_list = sorted(project_data.quarries.all(), key=lambda x: x.name)

    # Fetch data for each quarry
    B4C_results_data = {}
    sales_forecast_data = {}
    clusterToBox_df_change_data = {}

    for quarry in quarries_list:
        # Add capacityModes_df to the dictionary with quarry as key
        capacityModes_df_json = quarry.capacityModes_df_json
        capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
        B4C_results_data[quarry] = capacityModes_df

        # Add salesForecast_df to the dictionary with quarry as key
        salesForecast_df_json = quarry.salesForecast_df_json
        salesForecast_df = pd.read_json(StringIO(salesForecast_df_json), orient='records')
        sales_forecast_data[quarry] = salesForecast_df

        # Add clusterToBox_df_change to the dictionary with quarry as key
        clusterToBox_df_change = quarry.clusterToBox_df_change
        clusterToBox_df_change_data[quarry] = clusterToBox_df_change  # Store boolean data

    # Initialize lists to store quarry names, IDs, and conditions
    quarry_names = []
    quarry_ids = []
    conditions_B4C = []
    conditions_FarSeer = []
    clusterToBox_change = []

    # Iterate through the quarries and check if the sales forecast data is empty
    for quarry in quarries_list:
        quarry_names.append(quarry.name)
        quarry_ids.append(quarry.quarry_id)
        conditions_FarSeer.append(sales_forecast_data[quarry].empty)
        conditions_B4C.append(B4C_results_data[quarry].empty)
        clusterToBox_change.append(clusterToBox_df_change_data[quarry])  # Retrieve boolean data

    # Create a DataFrame with quarry names, IDs, and conditions
    FarSeer_empty_df = pd.DataFrame({
        'Quarry': quarry_names,
        'Quarry_ID': quarry_ids,
        'Condition_B4C': conditions_B4C,
        'Condition_FarSeer': conditions_FarSeer,
        'clusterToBox_change': clusterToBox_change,
    })

    # Count the number of unique quarries in the DataFrame
    num_quarries = FarSeer_empty_df['Quarry'].nunique()

    # Filter the DataFrame where both Condition and clusterToBox_change are False
    filtered_df = FarSeer_empty_df[(FarSeer_empty_df['Condition_B4C'] == False) & (FarSeer_empty_df['Condition_FarSeer'] == False) & (FarSeer_empty_df['clusterToBox_change'] == False)]

    # Count the occurrences
    count = filtered_df.shape[0]

    # Calculate P4P_completion
    FarSeer_completion = ((count / num_quarries) * 100)

    return FarSeer_completion


def NO_Completion(project_data):
    # Retrieve the list of quarries
    quarries_list = sorted(project_data.quarries.all(), key=lambda x: x.name)

    # Initiate data for each quarry
    B4C_results_data = {}
    P4P_results_data = {}
    clusterToBox_df_change_data = {}


    for quarry in quarries_list:
        # Add capacityModes_df to the dictionary with quarry as key
        capacityModes_df_json = quarry.capacityModes_df_json
        capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
        B4C_results_data[quarry] = capacityModes_df

        # Add P4PFullSummary_df to the dictionary with quarry as key
        P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
        P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
        P4P_results_data[quarry] = P4PFullSummary_df

        # Add clusterToBox_df_change to the dictionary with quarry as key
        clusterToBox_df_change = quarry.clusterToBox_df_change
        clusterToBox_df_change_data[quarry] = clusterToBox_df_change  # Store boolean data

    # Initialize lists to store quarry names, IDs, and conditions
    quarry_names = []
    quarry_ids = []
    conditions_B4C = []
    conditions_P4P = []
    clusterToBox_change = []


    # Iterate through the quarries and check if the sales forecast data is empty
    for quarry in quarries_list:
        quarry_names.append(quarry.name)
        quarry_ids.append(quarry.quarry_id)
        conditions_B4C.append(B4C_results_data[quarry].empty)
        conditions_P4P.append(P4P_results_data[quarry].empty)
        clusterToBox_change.append(clusterToBox_df_change_data[quarry])  # Retrieve boolean data


    # Create a DataFrame with quarry names, IDs, and conditions
    NO_Completion_df = pd.DataFrame({
        'Quarry': quarry_names,
        'Quarry_ID': quarry_ids,
        'Condition_B4C': conditions_B4C,
        'Condition_P4P': conditions_P4P,
        'clusterToBox_change': clusterToBox_change,
    })

    # Create the new columns
    NO_Completion_df['NO_Completion'] = ((1 - NO_Completion_df['Condition_P4P'].astype(int)) + (1 - NO_Completion_df['Condition_B4C'].astype(int))) * (1 - NO_Completion_df['clusterToBox_change'].astype(int)) # Convert True/False to 1/0

    NO_Completion_df['NO_Completion'] = (NO_Completion_df['NO_Completion'] / 2 * 100).astype(int)

    return NO_Completion_df