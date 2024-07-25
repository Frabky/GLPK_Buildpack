import pandas as pd
import plotly.express as px
import plotly.io as pio
import numpy as np



def P4P_Cost_Box_Table(PLAllocationData_df, costBoxProdVolume_df, clusterToBox_df):
    # Clean & format PLAllocationData_df
    PLAllocationData_df = PLAllocationData_df[PLAllocationData_df['Select'] == 'Checked']
    PLAllocationData_df = PLAllocationData_df.drop(columns=['Cost_Label', 'Select'])
    PLAllocationData_df['Cost'] = PLAllocationData_df['Cost'].replace('', 0)
    PLAllocationData_df['Cost'] = PLAllocationData_df['Cost'].astype(float).round(2) * 1000

    # Get the index of the "Cost_Type" column
    cost_type_index = PLAllocationData_df.columns.get_loc("Cost_Type")

    # Select columns after the "Cost_Type" column and multiply them by the values in the second column
    for col in PLAllocationData_df.columns[cost_type_index + 1:]:
        PLAllocationData_df[col] = PLAllocationData_df[col].replace('', 0)
        PLAllocationData_df[col] = PLAllocationData_df[col].astype(int)
        PLAllocationData_df[col] = PLAllocationData_df['Cost'] * PLAllocationData_df[col] / 100  # as it is %

    # Group by 'Cost_Type' and sum the values for each group
    PLAllocationData_df = PLAllocationData_df.pivot_table(index='Cost_Type', aggfunc='sum', fill_value=0)

    # If either 'Variable' or 'Fixed' group is missing, create the column and set the value to 0
    if 'Variable' not in PLAllocationData_df.index:
        PLAllocationData_df.loc['Variable'] = 0

    if 'Fixed' not in PLAllocationData_df.index:
        PLAllocationData_df.loc['Fixed'] = 0

    # Reset index to make 'Cost_Type' a column again
    PLAllocationData_df.reset_index(inplace=True)

    # Calculate the sum for each column
    total_values = PLAllocationData_df.sum(axis=0)

    # Create the dictionary for the 'Total' row
    total_row_dict = {'Cost_Type': ['Total']}
    for column_name in PLAllocationData_df.columns[1:]:  # Exclude the 'Cost_Type' column
        total_row_dict[column_name] = [total_values[column_name]]

    # Create the DataFrame for the 'Total' row
    total_row = pd.DataFrame(total_row_dict)

    # Append the 'Total' row to the original DataFrame
    PLAllocationData_df = pd.concat([PLAllocationData_df, total_row], ignore_index=True)

    # Transpose the dataframe
    costBoxValue_df = PLAllocationData_df.set_index('Cost_Type').T.reset_index()

    # Rename the 'index' column to 'Cost_Box'
    costBoxValue_df = costBoxValue_df.rename(columns={'index': 'Cost_Box'})

    # Check for leading or trailing whitespaces in the 'Cost_Box' column of costBoxValue_df
    costBoxValue_df['Cost_Box'] = costBoxValue_df['Cost_Box'].astype(str).str.strip()

    # Check for leading or trailing whitespaces in the 'Cost_Box' column of costBoxProdVolume_df
    costBoxProdVolume_df['Cost_Box'] = costBoxProdVolume_df['Cost_Box'].astype(str).str.strip()

    # Merge dataframes
    costBoxValue_df = pd.merge(costBoxValue_df, costBoxProdVolume_df, on='Cost_Box', how='inner')

    costBoxValue_df['Fixed_per_t'] = costBoxValue_df['Fixed'] / costBoxValue_df['Cost_Box_Prod_Volume']
    costBoxValue_df['Fixed_per_t'] = costBoxValue_df['Fixed_per_t'].round(2)
    costBoxValue_df['Variable_per_t'] = costBoxValue_df['Variable'] / costBoxValue_df['Cost_Box_Prod_Volume']
    costBoxValue_df['Variable_per_t'] = costBoxValue_df['Variable_per_t'].round(2)
    costBoxValue_df['Total_per_t'] = costBoxValue_df['Total'] / costBoxValue_df['Cost_Box_Prod_Volume']
    costBoxValue_df['Total_per_t'] = costBoxValue_df['Total_per_t'].round(2)

    # Construct cluster_to_box_value_df
    cluster_to_box_value_df = pd.merge(clusterToBox_df, costBoxValue_df, on='Cost_Box', how='inner')

    cluster_to_box_value_df['Value'] = cluster_to_box_value_df['Value'].astype(float)
    cluster_to_box_value_df['Cluster_Fixed_per_t'] = cluster_to_box_value_df['Fixed_per_t'] * cluster_to_box_value_df['Value'] / 100
    cluster_to_box_value_df['Cluster_Variable_per_t'] = cluster_to_box_value_df['Variable_per_t'] * cluster_to_box_value_df['Value'] / 100
    cluster_to_box_value_df['Cluster_Total_per_t'] = cluster_to_box_value_df['Total_per_t'] * cluster_to_box_value_df['Value'] / 100

    # Construction of first table
    summaryTableCostBox_df = cluster_to_box_value_df[['Cost_Box', 'Total', 'Cost_Box_Prod_Volume', 'Total_per_t', 'Product_Cluster', 'Cluster_Total_per_t']]

    # Pivot the data to have 'Cost_Box' as index, 'Product_Cluster' as columns, and 'Cluster_Total_per_t' as values
    pivot_df = summaryTableCostBox_df.pivot_table(index='Cost_Box', columns='Product_Cluster', values='Cluster_Total_per_t', fill_value=0)

    def format_value(value):
        if value == 0:
            return 0
        else:
            return "{:.2f}".format(round(value, 2))

    pivot_df *= -1
    pivot_df = pivot_df.apply(lambda x: x.map(format_value), axis=1)

    # Sort the 'Product_Cluster' values
    sorted_product_clusters = clusterToBox_df['Product_Cluster'].unique()
    sorted_cost_boxes = clusterToBox_df['Cost_Box'].unique()

    # Reindex the pivot DataFrame to ensure consistent order of 'Product_Cluster'
    pivot_df = pivot_df.reindex(index=sorted_cost_boxes, columns=sorted_product_clusters)

    # Convert the pivot DataFrame to a dictionary
    productClusterData = pivot_df.to_dict()

    # Drop columns
    summaryTableCostBox_df_dict = summaryTableCostBox_df.drop(columns=['Product_Cluster', 'Cluster_Total_per_t'])
    # Remove duplicate rows from the DataFrame
    summaryTableCostBox_df_dict = summaryTableCostBox_df_dict.drop_duplicates()
    # Convert the DataFrame to a dictionary with 'Cost_Box' as keys and other columns as values
    summaryTableCostBox_dict = summaryTableCostBox_df_dict.set_index('Cost_Box').transpose().to_dict()

    # Convert 'Cost_Box_Prod_Volume' values to integers in the dictionary
    for key in summaryTableCostBox_dict:
        summaryTableCostBox_dict[key]['Cost_Box_Prod_Volume'] = "{:,}".format(int(
            summaryTableCostBox_dict[key]['Cost_Box_Prod_Volume']))

    # Iterate over the dictionary and multiply each 'Total' value by -1
    for key in summaryTableCostBox_dict:
        if summaryTableCostBox_dict[key]['Total'] != '':
            summaryTableCostBox_dict[key]['Total'] = int(summaryTableCostBox_dict[key]['Total']) * -1

    # Iterate over the dictionary and format 'Total' values with thousands separators
    for key in summaryTableCostBox_dict:
        total_value = summaryTableCostBox_dict[key]['Total']
        if total_value != '':
            summaryTableCostBox_dict[key]['Total'] = "{:,}".format(total_value)

    # Iterate over the dictionary and multiply each value in the 'Total_per_t' key by -1
    for key in summaryTableCostBox_dict:
        summaryTableCostBox_dict[key]['Total_per_t'] *= -1
        # Format 'Total_per_t' value with two decimal places
        summaryTableCostBox_dict[key]['Total_per_t'] = format_value(summaryTableCostBox_dict[key]['Total_per_t'])

    return summaryTableCostBox_dict, cluster_to_box_value_df, sorted_product_clusters, productClusterData

def P4P_Cost_Box_Cluster_Table(cluster_to_box_value_df, sorted_product_clusters):
    # Construction of second table
    summaryTableCluster_df = cluster_to_box_value_df[['Product_Cluster', 'Cluster_Total_per_t', 'Cluster_Variable_per_t', 'Cluster_Fixed_per_t']]
    # Group by 'Product_Cluster' and calculate the sum for each group
    summaryTableCluster_df = summaryTableCluster_df.groupby('Product_Cluster').sum()
    # Reset index to make 'Product_Cluster' a regular column
    summaryTableCluster_df.reset_index(inplace=True)
    # Define the custom ordering for 'Product_Cluster'
    cat_dtype = pd.CategoricalDtype(categories=sorted_product_clusters, ordered=True)
    # Convert 'Product_Cluster' to the custom categorical data type
    summaryTableCluster_df['Product_Cluster'] = summaryTableCluster_df['Product_Cluster'].astype(cat_dtype)
    # Sort the DataFrame by 'Product_Cluster'
    summaryTableCluster_df.sort_values('Product_Cluster', inplace=True)

    def format_value(value):
        if value == 0:
            return 0
        else:
            return "{:.2f}".format(round(value, 2))

    # Multiply all values by -1
    summaryTableCluster_df[['Cluster_Total_per_t', 'Cluster_Variable_per_t', 'Cluster_Fixed_per_t']] *= -1

    # Apply the format_value function to round the values
    summaryTableCluster_df[['Cluster_Total_per_t', 'Cluster_Variable_per_t', 'Cluster_Fixed_per_t']] = \
        summaryTableCluster_df[['Cluster_Total_per_t', 'Cluster_Variable_per_t', 'Cluster_Fixed_per_t']].apply(
            lambda x: x.map(format_value))

    return summaryTableCluster_df



def P4P_Products_Table(productProductCluster_df, productProdVolume_df, modified_transactions_df, quarry, productAggLevy_df, summaryTableCluster_df):

    # Construction of summaryTableGeneral_df phase 1
    summaryTableGeneral_df = pd.merge(productProductCluster_df, productProdVolume_df, on='Product')

    # Construct temp_df
    temp_df = modified_transactions_df[['Quarries', 'Product', 'Sales_Volume', 'Revenue']].copy()
    temp_df = temp_df[temp_df['Quarries'] == quarry.name]
    temp_df = temp_df.groupby('Product').agg({'Sales_Volume': 'sum', 'Revenue': 'sum'}).reset_index()
    # Calculate ASP
    temp_df['ASP'] = (temp_df['Revenue'] / temp_df['Sales_Volume']).round(2)

    # Construction of summaryTableGeneral_df phase 2
    summaryTableGeneral_df = pd.merge(summaryTableGeneral_df, temp_df[['Product', 'Sales_Volume', 'ASP']], on='Product', how='left')
    summaryTableGeneral_df['Sales_Volume'] = summaryTableGeneral_df['Sales_Volume'].round().astype(int)
    summaryTableGeneral_df = pd.merge(summaryTableGeneral_df, productAggLevy_df, on='Product')
    summaryTableGeneral_df = pd.merge(summaryTableGeneral_df, summaryTableCluster_df[['Product_Cluster', 'Cluster_Variable_per_t', 'Cluster_Total_per_t']], on='Product_Cluster', how='left')
    summaryTableGeneral_df['Cluster_Variable_per_t'] = pd.to_numeric(summaryTableGeneral_df['Cluster_Variable_per_t'], errors='coerce')
    summaryTableGeneral_df['Cluster_Total_per_t'] = pd.to_numeric(summaryTableGeneral_df['Cluster_Total_per_t'], errors='coerce')
    summaryTableGeneral_df['Aggregate_Levy'] = pd.to_numeric(summaryTableGeneral_df['Aggregate_Levy'], errors='coerce')
    summaryTableGeneral_df['Gross Margin'] = (summaryTableGeneral_df['ASP'] - summaryTableGeneral_df['Aggregate_Levy'] - summaryTableGeneral_df['Cluster_Variable_per_t']).round(2)
    summaryTableGeneral_df['Total Margin'] = (summaryTableGeneral_df['ASP'] - summaryTableGeneral_df['Aggregate_Levy'] - summaryTableGeneral_df['Cluster_Total_per_t']).round(2)
    summaryTableGeneral_df.drop(columns=['Cluster_Variable_per_t', 'Cluster_Total_per_t'], inplace=True)
    summaryTableGeneral_df = summaryTableGeneral_df.rename(columns={'Production Volume': 'Production_Volume', 'Aggregate_Levy': 'Agg_levy', 'Gross Margin': 'GM', 'Total Margin': 'TM'})

    P4PFullSummary_df = summaryTableGeneral_df.copy()

    # Define a function to format numbers with ',' separator for thousands
    def format_with_commas(num):
        return "{:,.0f}".format(num)

    # Apply the formatting function to the desired columns
    summaryTableGeneral_df['Production_Volume'] = summaryTableGeneral_df['Production_Volume'].apply(format_with_commas)
    summaryTableGeneral_df['Sales_Volume'] = summaryTableGeneral_df['Sales_Volume'].apply(format_with_commas)

    return summaryTableGeneral_df, P4PFullSummary_df




def P4P_Sales_Margin_Vizualisation(modified_transactions_df, quarry, productProductCluster_df, summaryTableCluster_df, productAggLevy_df, project_data):
    temp2_df = modified_transactions_df[['Quarries', 'Customers', 'Product', 'Sales_Volume', 'Revenue']].copy()
    temp2_df = temp2_df[temp2_df['Quarries'] == quarry.name]

    # Grouping by specified columns and aggregating
    temp2_df = temp2_df.groupby(
        ['Quarries', 'Customers', 'Product']).agg({
        'Sales_Volume': 'sum',
        'Revenue': 'sum'
    }).reset_index()

    temp2_df['ASP'] = np.where(temp2_df['Sales_Volume'] != 0,
                               temp2_df['Revenue'] / temp2_df['Sales_Volume'],
                               0)  # You can replace 0 with np.nan or any other value as needed

    ChartAllCustomers_df = pd.merge(temp2_df, productProductCluster_df, on='Product', how='left')
    ChartAllCustomers_df = pd.merge(ChartAllCustomers_df, summaryTableCluster_df, on='Product_Cluster', how='left')
    ChartAllCustomers_df = pd.merge(ChartAllCustomers_df, productAggLevy_df, on='Product', how='left')
    ChartAllCustomers_df.fillna(0, inplace=True)

    # Convert columns to numeric type
    ChartAllCustomers_df['Cluster_Variable_per_t'] = pd.to_numeric(ChartAllCustomers_df['Cluster_Variable_per_t'], errors='coerce')
    ChartAllCustomers_df['Aggregate_Levy'] = pd.to_numeric(ChartAllCustomers_df['Aggregate_Levy'], errors='coerce')
    ChartAllCustomers_df['Cluster_Total_per_t'] = pd.to_numeric(ChartAllCustomers_df['Cluster_Total_per_t'], errors='coerce')

    # Calculate GM & TM
    ChartAllCustomers_df['GM_per_t'] = ChartAllCustomers_df['ASP'] - ChartAllCustomers_df['Cluster_Variable_per_t'] - ChartAllCustomers_df['Aggregate_Levy']
    ChartAllCustomers_df['TM_per_t'] = ChartAllCustomers_df['ASP'] - ChartAllCustomers_df['Cluster_Total_per_t'] - ChartAllCustomers_df['Aggregate_Levy']
    ChartAllCustomers_df['GM'] = ChartAllCustomers_df['Sales_Volume'] * ChartAllCustomers_df['GM_per_t']
    ChartAllCustomers_df['GM'] = ChartAllCustomers_df['GM'].astype(int)
    ChartAllCustomers_df['TM'] = ChartAllCustomers_df['Sales_Volume'] * ChartAllCustomers_df['TM_per_t']
    ChartAllCustomers_df['TM'] = ChartAllCustomers_df['TM'].astype(int)

    ChartAllCustomers_df['Sales_Volume'] = ChartAllCustomers_df['Sales_Volume'].astype(int)
    ChartAllCustomers_df['ASP'] = ChartAllCustomers_df['ASP'].round(2)
    ChartAllCustomers_df['GM_per_t'] = ChartAllCustomers_df['GM_per_t'].round(2)
    ChartAllCustomers_df['TM_per_t'] = ChartAllCustomers_df['TM_per_t'].round(2)


    currency = project_data.currency



    # Create interactive scatter plot Sales Price
    fig = px.scatter(ChartAllCustomers_df, x='Sales_Volume', y='ASP', hover_name='Customers', custom_data=['Product'])

    # Add custom hover information and set marker properties
    marker_color = ['#f5365c' if tm < 0 else '#172b4d' for tm in ChartAllCustomers_df['ASP']]

    fig.update_traces(
        hovertemplate=      '<b style="font-family: Segoe UI;">Customers:</b> <span style="font-family: Segoe UI;">%{hovertext}</span><br>'
                            '<b style="font-family: Segoe UI;">Product:</b> <span style="font-family: Segoe UI;">%{customdata[0]}</span><br>'
                            '<b style="font-family: Segoe UI;">Sales Volume:</b> <span style="font-family: Segoe UI;">%{x} t</span><br>'
                            '<b style="font-family: Segoe UI;">Sales Price:</b> <span style="font-family: Segoe UI;">%{y} ' + currency + '/t' '</span><br><extra></extra>',

        marker=dict(
            color=marker_color,  # Set point color to blue
            size=5  # Set point size
        )
    )

    # Update layout to change axis names, font, and set x-axis range
    fig.update_layout(
        title=dict(
            text="Sales Price Visualization",  # Specify chart title
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
            range=[0, round(ChartAllCustomers_df['Sales_Volume'].max() + 50)],  # Set y-axis range to start from 0
            gridcolor = '#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth = 1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',  # Specify color of zero axis line

        ),
        yaxis=dict(
            tickfont=dict(
                family='Segoe UI',  # Specify font family
                size=14,  # Specify font size
                color='#172b4d'  # Specify font color
            ),
            title=dict(
                text=f'Sales Price [{currency}/t]',  # Specify axis title text
                font=dict(
                    family='Segoe UI',  # Specify axis title font family
                    size=18,  # Specify axis title font size
                    color='#172b4d'  # Specify axis title font color
                )
            ),
            range=[round(ChartAllCustomers_df['ASP'].min() - 1), round(ChartAllCustomers_df['ASP'].max() + 1)],  # Set y-axis range to start from 0
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',
        ),
        plot_bgcolor='#f6f9fc',
        paper_bgcolor='#f6f9fc',  # Set background color around the chart
    )

    # Convert Plotly figure to HTML
    plotly_Sales_Price = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})







    # Create interactive scatter plot TM
    fig = px.scatter(ChartAllCustomers_df, x='Sales_Volume', y='TM_per_t', hover_name='Customers',
                     custom_data=['Product', 'ASP', 'TM'])

    # Add custom hover information and set marker properties
    marker_color = ['#dc3545' if tm < 0 else '#172b4d' for tm in ChartAllCustomers_df['TM_per_t']]

    fig.update_traces(
            hovertemplate=  '<b style="font-family: Segoe UI;">Customers:</b> <span style="font-family: Segoe UI;">%{hovertext}</span><br>'
                            '<b style="font-family: Segoe UI;">Product:</b> <span style="font-family: Segoe UI;">%{customdata[0]}</span><br>'
                            '<b style="font-family: Segoe UI;">Sales Volume:</b> <span style="font-family: Segoe UI;">%{x} t</span><br>'
                            '<b style="font-family: Segoe UI;">Sales Price:</b> <span style="font-family: Segoe UI;">%{customdata[1]} ' + currency + '</span><br>'  # Include currency here
                            '<b style="font-family: Segoe UI;">Total Margin:</b> <span style="font-family: Segoe UI;">%{y} ' + currency + '/t' '</span><br>'  # Include currency here
                            '<b style="font-family: Segoe UI;">Total Margin:</b> <span style="font-family: Segoe UI;">%{customdata[2]} ' + currency + '</span><br><extra></extra>',

        marker=dict(
            color=marker_color,  # Set point color to blue
            size=5  # Set point size
        )
    )

    # Update layout to change axis names, font, and set x-axis range
    fig.update_layout(
        title=dict(
            text="Total Margin Visualization",  # Specify chart title
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
            range=[0, round(ChartAllCustomers_df['Sales_Volume'].max() + 50)],  # Set y-axis range to start from 0
            gridcolor = '#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth = 1,  # Specify width of x-axis gridlines
            zerolinecolor = '#e3e3e3',  # Specify color of zero axis line

    ),
        yaxis=dict(
            tickfont=dict(
                family='Segoe UI',  # Specify font family
                size=14,  # Specify font size
                color='#172b4d'  # Specify font color
            ),
            title=dict(
                text=f'Total Margin [{currency}/t]',  # Specify axis title text
                font=dict(
                    family='Segoe UI',  # Specify axis title font family
                    size=18,  # Specify axis title font size
                    color='#172b4d'  # Specify axis title font color
                )
            ),
            range=[round(ChartAllCustomers_df['TM_per_t'].min() - 1), round(ChartAllCustomers_df['TM_per_t'].max() + 1)], # Set y-axis range to start from 0
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',  # Specify color of zero axis line
        ),
        plot_bgcolor='#f6f9fc',
        paper_bgcolor='#f6f9fc',  # Set background color around the chart
    )

    # Convert Plotly figure to HTML
    plotly_TM = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})





    # Create interactive scatter plot GM
    fig = px.scatter(ChartAllCustomers_df, x='Sales_Volume', y='GM_per_t', hover_name='Customers', custom_data=['Product', 'ASP', 'GM'])

    # Add custom hover information and set marker properties
    marker_color = ['#dc3545' if gm < 0 else '#172b4d' for gm in ChartAllCustomers_df['GM_per_t']]

    fig.update_traces(
        hovertemplate=      '<b style="font-family: Segoe UI;">Customers:</b> <span style="font-family: Segoe UI;">%{hovertext}</span><br>'
                            '<b style="font-family: Segoe UI;">Product:</b> <span style="font-family: Segoe UI;">%{customdata[0]}</span><br>'
                            '<b style="font-family: Segoe UI;">Sales Volume:</b> <span style="font-family: Segoe UI;">%{x} t</span><br>'
                            '<b style="font-family: Segoe UI;">Sales Price:</b> <span style="font-family: Segoe UI;">%{customdata[1]} ' + currency + '</span><br>'  # Include currency here
                            '<b style="font-family: Segoe UI;">Gross Margin:</b> <span style="font-family: Segoe UI;">%{y} ' + currency + '/t' '</span><br>'  # Include currency here
                            '<b style="font-family: Segoe UI;">Gross Margin:</b> <span style="font-family: Segoe UI;">%{customdata[2]} ' + currency + '</span><br><extra></extra>',  # Include currency here

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
            range=[0, round(ChartAllCustomers_df['Sales_Volume'].max() + 50)],  # Set y-axis range to start from 0
            gridcolor = '#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth = 1,  # Specify width of x-axis gridlines
            zerolinecolor = '#e3e3e3',  # Specify color of zero axis line
        ),
        yaxis=dict(
            tickfont=dict(
                family='Segoe UI',  # Specify font family
                size=14,  # Specify font size
                color='#172b4d'  # Specify font color
            ),
            title=dict(
                text=f'Gross Margin [{currency}/t]',  # Specify axis title text
                font=dict(
                    family='Segoe UI',  # Specify axis title font family
                    size=18,  # Specify axis title font size
                    color='#172b4d'  # Specify axis title font color
                )
            ),
            range=[round(ChartAllCustomers_df['GM_per_t'].min() - 1), round(ChartAllCustomers_df['GM_per_t'].max() + 1)], # Set y-axis range to start from 0
            gridcolor='#e3e3e3',  # Specify color of x-axis gridlines
            gridwidth=1,  # Specify width of x-axis gridlines
            zerolinecolor='#e3e3e3',  # Specify color of zero axis line
        ),
        plot_bgcolor='#f6f9fc',
        paper_bgcolor='#f6f9fc',  # Set background color around the chart
    )
    # Convert Plotly figure to HTML
    plotly_GM = pio.to_html(fig, include_plotlyjs=False, full_html=False, config={'displayModeBar': False})


    return plotly_Sales_Price, plotly_TM, plotly_GM, ChartAllCustomers_df


def P4P_Negative_Margin_Table(ChartAllCustomers_df):
    # Define a function to format numbers with ',' separator for thousands
    def format_with_commas(num):
        return "{:,.0f}".format(num)

    # Reformating the dataframe
    Margin_Table_df = ChartAllCustomers_df.drop(columns=['Quarries']).copy()
    Margin_Table_df = Margin_Table_df.rename(columns={'Sales Price': 'Sales_Price'})
    Margin_Table_df['GM_per_t'] = Margin_Table_df['GM_per_t'].round(2)
    Margin_Table_df['TM_per_t'] = Margin_Table_df['TM_per_t'].round(2)
    Margin_Table_df['ASP'] = Margin_Table_df['ASP'].round(2)
    Margin_Table_df['Sales_Volume'] = Margin_Table_df['Sales_Volume'].astype(int).apply(format_with_commas)

    Negative_Margin_Table_df = Margin_Table_df.sort_values(by=['TM'], ascending=True)
    Negative_Margin_Table_df = Negative_Margin_Table_df[Negative_Margin_Table_df['TM'] < 0].reset_index()

    # Count of negative GM
    count_neg_GM = len(Negative_Margin_Table_df[Negative_Margin_Table_df['GM'] < 0])
    # Calculate the sum of GM for those lines
    sum_neg_GM = Negative_Margin_Table_df[Negative_Margin_Table_df['GM'] < 0]['GM'].sum()

    # Format
    Negative_Margin_Table_df['TM'] = Negative_Margin_Table_df['TM'].astype(int).apply(format_with_commas)

    sum_neg_GM = format_with_commas(sum_neg_GM)

    return Margin_Table_df, Negative_Margin_Table_df, count_neg_GM, sum_neg_GM


def P4P_Download_Data (modified_transactions_df, quarry, productProductCluster_df,summaryTableCluster_df, productAggLevy_df, currency):
    temp2_df = modified_transactions_df[['Quarries', 'Customers', 'Product', 'Sales_Volume', 'Revenue']].copy()
    temp2_df = temp2_df[temp2_df['Quarries'] == quarry.name]

    # Grouping by specified columns and aggregating
    temp2_df = temp2_df.groupby(
        ['Quarries', 'Customers', 'Product']).agg({
        'Sales_Volume': 'sum',
        'Revenue': 'sum'
    }).reset_index()

    temp2_df['ASP'] = np.where(temp2_df['Sales_Volume'] != 0,
                               temp2_df['Revenue'] / temp2_df['Sales_Volume'],
                               0)  # You can replace 0 with np.nan or any other value as needed

    Download_df = pd.merge(temp2_df, productProductCluster_df, on='Product', how='left')
    Download_df = pd.merge(Download_df, summaryTableCluster_df, on='Product_Cluster', how='left')
    Download_df = pd.merge(Download_df, productAggLevy_df, on='Product', how='left')
    Download_df.fillna(0, inplace=True)

    # Convert columns to numeric type
    Download_df['Cluster_Total_per_t'] = pd.to_numeric(Download_df['Cluster_Total_per_t'], errors='coerce')
    Download_df['Cluster_Variable_per_t'] = pd.to_numeric(Download_df['Cluster_Variable_per_t'], errors='coerce')
    Download_df['Cluster_Fixed_per_t'] = Download_df['Cluster_Total_per_t'] - Download_df['Cluster_Variable_per_t']

    # Calculate GM & TM
    Download_df['GM_per_t'] = Download_df['ASP'] - Download_df['Cluster_Variable_per_t'] - Download_df['Aggregate_Levy']
    Download_df['TM_per_t'] = Download_df['ASP'] - Download_df['Cluster_Total_per_t'] - Download_df['Aggregate_Levy']
    Download_df['GM'] = Download_df['Sales_Volume'] * Download_df['GM_per_t']
    Download_df['GM'] = Download_df['GM'].round(0).astype(int)
    Download_df['TM'] = Download_df['Sales_Volume'] * Download_df['TM_per_t']
    Download_df['TM'] = Download_df['TM'].round(0).astype(int)

    columns_order = ['Customers', 'Product', 'Product_Cluster', 'Sales_Volume', 'ASP', 'Revenue', 'Aggregate_Levy', 'Cluster_Variable_per_t',
                     'Cluster_Fixed_per_t', 'Cluster_Total_per_t', 'GM_per_t', 'TM_per_t', 'GM', 'TM']

    Download_df = Download_df[columns_order]

    # Format
    Download_df['Sales_Volume'] = Download_df['Sales_Volume'].round(1)
    Download_df = Download_df.rename(columns={'Sales_Volume': 'Sales Volume [t]'})

    Download_df['ASP'] = Download_df['ASP'].round(2)
    Download_df = Download_df.rename(columns={'ASP': 'Sales Price ' + '[' + currency +'/t]'})

    Download_df = Download_df.rename(columns={'Product_Cluster': 'Product Cluster'})

    Download_df['Cluster_Total_per_t'] = Download_df['Cluster_Total_per_t'].round(2)
    Download_df = Download_df.rename(columns={'Cluster_Total_per_t': 'Total Cost ' + '[' + currency + '/t]'})

    Download_df['Cluster_Variable_per_t'] = Download_df['Cluster_Variable_per_t'].round(2)
    Download_df = Download_df.rename(columns={'Cluster_Variable_per_t': 'Variable Cost' + '[' + currency + '/t]'})

    Download_df['Cluster_Fixed_per_t'] = Download_df['Cluster_Fixed_per_t'].round(2)
    Download_df = Download_df.rename(columns={'Cluster_Fixed_per_t': 'Fixed Cost ' + '[' + currency + '/t]'})

    Download_df['Aggregate_Levy'] = Download_df['Aggregate_Levy'].round(2)
    Download_df = Download_df.rename(columns={'Aggregate_Levy': 'Aggregate Levy ' + '[' + currency + '/t]'})

    Download_df['GM_per_t'] = Download_df['GM_per_t'].round(2)
    Download_df = Download_df.rename(columns={'GM_per_t': 'Gross Margin ' + '[' + currency + '/t]'})

    Download_df['TM_per_t'] = Download_df['TM_per_t'].round(2)
    Download_df = Download_df.rename(columns={'TM_per_t': 'Total Margin ' + '[' + currency + '/t]'})

    Download_df['GM'] = Download_df['GM'].astype(int)
    Download_df = Download_df.rename(columns={'GM': 'Gross Margin ' + '[' + currency + ']'})

    Download_df['TM'] = Download_df['TM'].astype(int)
    Download_df = Download_df.rename(columns={'TM': 'Total Margin ' + '[' + currency + ']'})

    return Download_df