import pandas as pd
import re
import folium
from folium.plugins import HeatMap
import json
import numpy as np
from django.templatetags.static import static
import os
from django.conf import settings




def Products_extraction (filtered_df):
    # Keep only two specific columns by name
    products_df = filtered_df[['Product', 'Sales_Volume']]

    # Grouping by specified columns and aggregating with sum of 'Sales_Volume'
    products_df = products_df.groupby(['Product']).agg({
        'Sales_Volume': 'sum'
    }).reset_index()

    products_df['Sales_Volume'] = products_df['Sales_Volume'].astype(int)

    # Sorting based on the summed 'Sales_Volume' column in descending order
    products_df = products_df.sort_values(by='Sales_Volume', ascending=False)

    return products_df


def Modified_products_integration(filtered_df, modifiedProducts):
    modifiedProducts_df = pd.DataFrame(modifiedProducts)
    modifiedProducts_df = modifiedProducts_df[['Product', 'Modification', 'Product Group']]

    # Merge the DataFrames based on the 'Product' column
    modified_transactions_df = pd.merge(filtered_df, modifiedProducts_df, on='Product', how='left')

    # Removing rows where 'Modification' column contains 'Removed'
    modified_transactions_df = modified_transactions_df[modified_transactions_df['Modification'] != 'Removed']

    # Function to extract new product name from 'Modification'
    def extract_new_product(modification_text):
        if isinstance(modification_text, str):
            match = re.search(r'Renamed as (.+)', modification_text)
            return match.group(1) if match else None
        else:
            return None

    # Apply the function to extract new product names from 'Modification'
    modified_transactions_df['New Product'] = modified_transactions_df['Modification'].apply(extract_new_product)

    # Copy Product column containing original product name and rename the copied column as "Old product name"
    modified_transactions_df['Old product name'] = modified_transactions_df['Product'].copy()

    # Replace 'Product' values where 'Modification' contains 'Renamed as {product}'
    mask = modified_transactions_df['Modification'].fillna('').str.contains('Renamed as')
    modified_transactions_df.loc[mask, 'Product'] = modified_transactions_df.loc[mask, 'New Product']

    # Drop the 'New Product' and 'Modification' column
    modified_transactions_df.drop(columns='New Product', inplace=True)
    modified_transactions_df.drop(columns='Modification', inplace=True)


    # Splitting 'Customers location coordinates'
    split_coordinates = modified_transactions_df['Customers location coordinates'].apply(
        lambda x: pd.Series(json.loads(x) if isinstance(x, str) and pd.notna(x) else [np.nan, np.nan]))


    # Ensure split_coordinates has two columns
    if len(split_coordinates.columns) == 2:
        # Assign the new columns to modified_transactions_df
        modified_transactions_df['Customers latitude'] = split_coordinates[0].astype(float)
        modified_transactions_df['Customers longitude'] = split_coordinates[1].astype(float)
    else:
        print("Error: split_coordinates does not have two columns")

    # Drop the 'Customers location coordinates' column
    modified_transactions_df.drop('Customers location coordinates', axis=1, inplace=True)


    # Splitting 'Quarries coordinates'
    split_quarries_coordinates = modified_transactions_df['Quarries coordinates'].apply(
        lambda x: pd.Series(json.loads(x) if isinstance(x, str) and pd.notna(x) else [np.nan, np.nan]))

    # Ensure split_quarries_coordinates has two columns
    if len(split_quarries_coordinates.columns) == 2:
        # Assign the new columns to modified_transactions_df
        modified_transactions_df['Quarries latitude'] = split_quarries_coordinates[0].astype(float)
        modified_transactions_df['Quarries longitude'] = split_quarries_coordinates[1].astype(float)
    else:
        print("Error: split_quarries_coordinates does not have two columns")

    # Drop the 'Quarries coordinates' column
    modified_transactions_df.drop('Quarries coordinates', axis=1, inplace=True)

    # Round the 'Sales_Volume' column values to the nearest decimal
    modified_transactions_df['Sales_Volume'] = modified_transactions_df['Sales_Volume'].round(1)

    # Replace commas with periods
    modified_transactions_df['Product'] = modified_transactions_df['Product'].str.replace(',', '.', regex=False)

    # Create modified_transactions_day_df
    modified_transactions_day_df = modified_transactions_df.copy()


    # Drop "Jour" & "ASP" columns
    modified_transactions_df.drop(columns='Day', inplace=True)

    # Grouping by specified columns and aggregating
    modified_transactions_df = modified_transactions_df.groupby(['Quarries', 'Incoterms', 'Location', 'Product', 'Customers', 'Customers location', 'Product Group', 'Old product name', 'Customers latitude', 'Customers longitude',
       'Quarries latitude', 'Quarries longitude']).agg({
        'Sales_Volume': 'sum',
        'Revenue': 'sum'
    }).reset_index()

    # Adding the ASP
    modified_transactions_df['Average sales price'] = round(modified_transactions_df['Revenue'] / modified_transactions_df['Sales_Volume'], 2)

    return modified_transactions_df, modified_transactions_day_df


def Heatmap(modified_transactions_df):
    # Select only relevant columns
    Heatmap_df = modified_transactions_df[["Customers latitude", "Customers longitude", "Product Group", "Sales_Volume"]]

    # Group by both 'Location' and 'Product_Group' and sum the 'Sales_Volume' for each group
    Heatmap_df_grouped = Heatmap_df.groupby(['Customers latitude', 'Customers longitude', 'Product Group'])['Sales_Volume'].sum().reset_index()

    # Calculate mean for latitude and longitude for Quarries
    Mean_latitude_quarries = modified_transactions_df['Quarries latitude'].mean()
    Mean_longitude_quarries = modified_transactions_df['Quarries longitude'].mean()

    # Create the base map for the Heatmap with no filter
    Heatmap_all_product_groups = folium.Map(location=[Mean_latitude_quarries, Mean_longitude_quarries], zoom_start=5)

    # Add a weighted heatmap layer using 'Latitude', 'Longitude', and 'Sales_Volume' from 'Heatmap_df_grouped'
    locations_with_weights = [(latitude, longitude, float(volume)) for latitude, longitude, volume in
                              zip(Heatmap_df_grouped['Customers latitude'], Heatmap_df_grouped['Customers longitude'],
                                  Heatmap_df_grouped['Sales_Volume'])]

    # Get unique quarries
    unique_quarries = modified_transactions_df['Quarries'].unique()

    # Initialize an empty list to store precise locations
    precise_locations = []

    # Iterate over unique quarries
    for quarry in unique_quarries:
        # Filter the DataFrame for the current quarry
        quarry_df = modified_transactions_df[modified_transactions_df['Quarries'] == quarry]

        # Retrieve coordinates for the current quarry
        coordinates = (quarry_df.iloc[0]['Quarries latitude'], quarry_df.iloc[0]['Quarries longitude'])

        # Add the quarry and its coordinates to the list with a default weight of 50
        precise_locations.append({'name': quarry, 'coordinates': coordinates, 'weight': 50})

    # Path to the custom icon
    custom_icon_path = os.path.join(settings.STATIC_ROOT, 'QuarryIQ', 'images', 'Quarry_sign.png')

    # Add precise locations with custom icons
    for location_info in precise_locations:
        custom_icon = folium.features.CustomIcon(custom_icon_path, icon_size=(20, 20))
        folium.Marker(
            location=location_info['coordinates'],
            popup=location_info['name'],
            icon=custom_icon,
            weight=location_info['weight']
        ).add_to(Heatmap_all_product_groups)

    HeatMap(locations_with_weights, radius=20, blur=15).add_to(Heatmap_all_product_groups)

    # Get unique Product Groups
    unique_product_groups = Heatmap_df_grouped['Product Group'].unique()

    # Dictionary to store heatmaps for each product group
    heatmaps = {'All_Product_Groups': Heatmap_all_product_groups}

    # Generate heatmap for each product group
    for product_group in unique_product_groups:
        # Create a dataframe for each product group
        df_for_product_group = Heatmap_df_grouped[Heatmap_df_grouped['Product Group'] == product_group].copy()

        # Create a base map
        heatmap_map = folium.Map(location=[Mean_latitude_quarries, Mean_longitude_quarries], zoom_start=5)

        # Add precise locations with custom icons to the heatmap for the current product group
        for location_info in precise_locations:
            custom_icon = folium.features.CustomIcon(custom_icon_path, icon_size=(20, 20))
            folium.Marker(
                location=location_info['coordinates'],
                popup=location_info['name'],
                icon=custom_icon,
                weight=location_info['weight']
            ).add_to(heatmap_map)

        # Add a weighted heatmap layer using 'Latitude', 'Longitude', and 'Sales_Volume' from 'df_for_product_group'
        locations_with_weights = [(latitude, longitude, float(volume)) for latitude, longitude, volume in
                                  zip(df_for_product_group['Customers latitude'],
                                      df_for_product_group['Customers longitude'],
                                      df_for_product_group['Sales_Volume'])]

        HeatMap(locations_with_weights, radius=20, blur=15).add_to(heatmap_map)

        # Store heatmap in the dictionary with product group as key
        heatmaps[product_group] = heatmap_map

    return heatmaps


def Create_stream_map(dataframe, incoterm_filter=None):
    # Select only relevant columns
    Streams_df = dataframe[
        ["Quarries", "Quarries latitude", "Quarries longitude", "Customers", "Customers latitude",
         "Customers longitude", "Sales_Volume", "Incoterms"]]

    # If incoterm_filter is provided, filter the DataFrame
    if incoterm_filter:
        Streams_df = Streams_df[Streams_df['Incoterms'] == incoterm_filter]

    # Group by 'Customers' and aggregate relevant columns
    Streams_df = Streams_df.groupby('Customers').agg({
        'Quarries': 'first',
        'Quarries latitude': 'first',
        'Quarries longitude': 'first',
        'Customers latitude': 'first',
        'Customers longitude': 'first',
        'Incoterms': 'sum',
        'Sales_Volume': 'sum'
    }).reset_index()

    # Sort DataFrame by Sales_Volume in descending order and select top 1000 rows
    Streams_df = Streams_df.sort_values(by='Sales_Volume', ascending=False).head(250)

    # Create a dictionary to store unique coordinates and their corresponding customers
    unique_coordinates = {}

    # Create a new map for each incoterm filter
    Streams_map = folium.Map(
        location=[Streams_df['Quarries latitude'].mean(), Streams_df['Quarries longitude'].mean()],
        zoom_start=5)

    # Iterate through each row in the Streams_df DataFrame
    for index, row in Streams_df.iterrows():
        # Check for NaN values in coordinates
        if pd.notna(row['Quarries latitude']) and pd.notna(row['Quarries longitude']) and pd.notna(
                row['Customers latitude']) and pd.notna(row['Customers longitude']):
            # Create a line connecting the origin and destination
            line = folium.PolyLine(
                locations=[
                    (row['Quarries latitude'], row['Quarries longitude']),
                    (row['Customers latitude'], row['Customers longitude'])
                ],
                color='red',
                weight=row['Sales_Volume'] / 3000,  # Adjust the line weight based on volume
            )

            Streams_map.add_child(line)

            # Add custom icon for customers with pop-up information
            customer_coordinates = (row['Customers latitude'], row['Customers longitude'])

            # If coordinates are already present, slightly adjust them
            while customer_coordinates in unique_coordinates:
                customer_coordinates = (
                    customer_coordinates[0] + 0.0001,
                    customer_coordinates[1] + 0.0001
                )

            unique_coordinates[customer_coordinates] = row['Customers']

            # Path to the custom icon
            customer_icon_path = os.path.join(settings.STATIC_ROOT, 'QuarryIQ', 'images', 'Customer.png')
            quarry_icon_path = os.path.join(settings.STATIC_ROOT, 'QuarryIQ', 'images', 'Quarry_sign.png')

            customer_icon = folium.Marker(
                location=customer_coordinates,
                icon=folium.CustomIcon(customer_icon_path, icon_size=(20, 20)),
                popup=f"Customer: {row['Customers']} Sales_Volume: {round(row['Sales_Volume'],1)}"
            )
            Streams_map.add_child(customer_icon)

            # Add custom icon for quarries with pop-up information
            quarry_icon = folium.Marker(
                location=(row['Quarries latitude'], row['Quarries longitude']),
                icon=folium.CustomIcon(quarry_icon_path, icon_size=(20, 20)),
                popup=f"Quarry: {row['Quarries']}"
            )
            Streams_map.add_child(quarry_icon)

    return Streams_map


def Streamsmap_all_incoterms(modified_transactions_df):
    # Extract unique incoterms
    unique_incoterms = modified_transactions_df['Incoterms'].unique()

    # Create maps for all Incoterms
    return Create_stream_map(modified_transactions_df)


def Streamsmap_incoterm(modified_transactions_df, incoterms):
    # Create an empty dictionary to store the maps for each incoterm
    streams_maps = {}

    # Loop through each incoterm
    for incoterm in incoterms:
        streams_maps[incoterm] = Create_stream_map(modified_transactions_df, incoterm_filter=incoterm)
    return streams_maps