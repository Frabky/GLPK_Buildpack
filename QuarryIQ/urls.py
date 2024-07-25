from django.urls import path
from . import views
from django.contrib.auth import views as auth_views
from django.contrib import admin





urlpatterns = [
    path('admin/', admin.site.urls),
    path('csp-report-endpoint/', views.csp_report, name='csp_report'),

    # Sagitta Nova website
    path("", views.Home, name='Home'),
    path("Contact/", views.Contact, name='Contact'),

    # Quarry IQ application
    # Login / Password
    path("Login/", auth_views.LoginView.as_view(template_name='QuarryIQ/Login.html'), name='Login'),
    path('Logout/', auth_views.LogoutView.as_view(template_name='QuarryIQ/Logout.html'), name='Logout'),
    path('User_Profile/', views.User_Profile_view, name='User_Profile'),


    # Admin Portal
    path('Admin_Portal', views.Admin_Portal_view, name='Admin_Portal'),
    path('Delete_Access/<int:association_id>/', views.Delete_Access_bridge, name='Delete_Access'),
    path('Delete_Project/<str:project_id>/', views.Delete_Project_bridge, name='Delete_Project'),
    path('New_Project/General_Info/', views.New_Project_General_Info_view, name='New_Project_General_Info'),
    path('New_Project/General_Info_bridge/<str:project_id>/', views.General_Info_to_Data_Cleaning_bridge, name='General_Info_to_Data_Cleaning'),
    path('New_Project/Upload_Data/<str:project_id>/', views.Upload_Data, name='Upload_Data'),

    path('Model_Portal', views.Model_Portal_view, name='Model_Portal'),
    path('flush-uploaded-glpk/', views.Flush_uploaded_glpk, name='flush_uploaded_glpk'),


    # Project List
    path('Project_List/', views.Project_List_view, name='Project_List'),
    path('Modify_Project_Information/<str:project_id>/', views.Modify_Information_view, name='Modify_Information'),


    # Existing Projects / Data Portal
    path('Existing_Projects/Data_Portal/<str:project_id>/', views.Data_Portal_view, name='Data_Portal'),

    # Existing Projects / Data Portal / Product Cleaning
    path('Existing_Projects/Data_Portal/Product_Cleaning/<str:project_id>/', views.Product_Cleaning_view, name='Product_Cleaning'),
    path('Transfer_point/<str:project_id>/', views.Handle_modified_products_view, name='Handle_modified_products'),

    # Existing Projects / Data Portal / Heatmaps
    path('Existing_Projects/Data_Portal/Heatmaps/Heatmap_Portal/<str:project_id>/', views.Heatmap_Portal_view, name='Heatmap_Portal'),
    path('Existing_Projects/Data_Portal/Heatmaps/Heatmap_all_product_group/<str:project_id>/', views.Heatmap_all_product_groups_view, name='Heatmap_all_product_groups'),
    path('Existing_Projects/Data_Portal/Heatmaps/Heatmap_product_group/<str:product_group>/<str:project_id>/', views.Heatmap_product_group_view, name='Heatmap_product_group'),

    # Existing Projects / Data Portal / Streamsmaps
    path('Existing_Projects/Data_Portal/Heatmaps/Streams_map_Portal/<str:project_id>/', views.Streamsmap_Portal_view, name='Streamsmap_Portal'),
    path('Existing_Projects/Data_Portal/Streamsmaps/Streams_maps_baseline_all_incoterms/<str:project_id>/', views.Streamsmap_baseline_all_incoterms_view, name='Streamsmap_baseline_all_incoterms'),
    path('Existing_Projects/Data_Portal/Streamsmaps/Streams_maps_baseline_incoterm/<str:incoterm>/<str:project_id>/', views.Streams_maps_baseline_incoterm_view, name='Streams_maps_baseline_incoterm_view'),

    # Existing Projects / Data Portal / P4P Portal
    path('Existing_Projects/Data_Portal/P4P_Portal/<str:project_id>/', views.P4P_Portal_view, name='P4P_Portal'),
    path('Existing_Projects/Data_Portal/P4P_Portal/Cluster_to_box/<str:project_id>/<str:quarry_id>/', views.P4P_Cluster_to_box_view, name='P4P_Cluster_to_box'),
    path('Existing_Projects/Data_Portal/P4P_Portal/Cluster_to_box_bridge/<str:project_id>/<str:quarry_id>/', views.P4P_Cluster_to_box_to_Product_list_bridge, name='P4P_Cluster_to_box_to_Product_list'),
    path('Existing_Projects/Data_Portal/P4P_Portal/Product_list/<str:project_id>/<str:quarry_id>/', views.P4P_Product_list_view, name='P4P_Product_list'),
    path('Existing_Projects/Data_Portal/P4P_Portal/Product_list_bridge/<str:project_id>/<str:quarry_id>/', views.P4P_Product_list_to_PL_allocation_bridge, name='P4P_Product_list_to_PL_allocation'),
    path('Existing_Projects/Data_Portal/P4P_Portal/PL_allocation/<str:project_id>/<str:quarry_id>/', views.P4P_PL_allocation_view, name='P4P_PL_allocation'),
    path('Existing_Projects/Data_Portal/P4P_Portal/Electricity_calculator/<str:project_id>/<str:quarry_id>/', views.Electricity_calculator_view, name='P4P_Elec_calculator'),
    path('Existing_Projects/Data_Portal/P4P_Portal/PL_allocation_bridge/<str:project_id>/<str:quarry_id>/', views.P4P_PL_allocation_to_Results_bridge, name='P4P_PL_allocation_to_Results'),
    path('Existing_Projects/Data_Portal/P4P_Portal/P4P_Results/<str:project_id>/<str:quarry_id>/', views.P4P_Results_view, name='P4P_Results'),

    # Existing Projects / Data Portal / B4P Portal
    path('Existing_Projects/Data_Portal/B4C_Portal/<str:project_id>/', views.B4C_Portal_view, name='B4C_Portal'),
    path('Existing_Projects/Data_Portal/B4C_Portal/Product_Composition/<str:project_id>/<str:quarry_id>/', views.B4C_Product_Composition_view, name='B4C_Product_Composition'),
    path('Existing_Projects/Data_Portal/B4C_Portal/Product_Composition_bridge/<str:project_id>/<str:quarry_id>/', views.B4C_Product_Composition_to_Production_Balance_bridge, name='B4C_Product_Composition_to_Production_Balance'),
    path('Existing_Projects/Data_Portal/B4C_Portal/Production_Balance/<str:project_id>/<str:quarry_id>/', views.B4C_Production_Balance_view, name='B4C_Production_Balance_view'),
    path('Existing_Projects/Data_Portal/B4C_Portal/Production_Balance_bridge/<str:project_id>/<str:quarry_id>/', views.B4C_Production_Balance_to_Results_bridge, name='B4C_Production_Balance_to_Results'),
    path('Existing_Projects/Data_Portal/B4C_Portal/Results/<str:project_id>/<str:quarry_id>/', views.B4C_Results_view, name='B4C_Results'),
    path('Existing_Projects/Data_Portal/B4C_Portal/Simulator/<str:project_id>/<str:quarry_id>/', views.B4C_Simulator_view, name='B4C_Simulator'),
    path('Existing_Projects/Data_Portal/B4C_Portal/Simulator_bridge/<str:project_id>/<str:quarry_id>/', views.B4C_Simulator_bridge, name='B4C_Simulator_bridge'),

    # Existing Projects / Data Portal / FarSeer Portal
    path('Existing_Projects/Data_Portal/FarSeer_Portal/<str:project_id>/', views.FarSeer_Portal_view, name='FarSeer_Portal'),
    path('Existing_Projects/Data_Portal/FarSeer_Portal/<str:project_id>/<str:quarry_id>/', views.FarSeer_Clear_Data, name='FarSeer_Clear_Data'),
    path('Existing_Projects/Data_Portal/FarSeer_Portal/Informations/<str:project_id>/<str:quarry_id>/', views.FarSeer_Information_view, name='FarSeer_Information'),
    path('Existing_Projects/Data_Portal/FarSeer_Portal/Informations_bridge/<str:project_id>/<str:quarry_id>/', views.FarSeer_Information_to_Forecast_bridge, name='FarSeer_Information_to_Forecast'),
    path('Existing_Projects/Data_Portal/FarSeer_Portal/Forecast/<str:project_id>/<str:quarry_id>/', views.FarSeer_Forecast_view, name='FarSeer_Forecast'),
    path('Existing_Projects/Data_Portal/FarSeer_Portal/Forecast_bridge/<str:project_id>/<str:quarry_id>/', views.FarSeer_Forecast_to_Results_bridge, name='FarSeer_Forecast_to_Results'),
    path('Existing_Projects/Data_Portal/FarSeer_Portal/Results_Summary/<str:project_id>/<str:quarry_id>/', views.FarSeer_Results_Summary_view, name='FarSeer_Results_Summary'),
    path('Existing_Projects/Data_Portal/FarSeer_Portal/Results_Summary_Meet_All_Sales/<str:project_id>/<str:quarry_id>/', views.FarSeer_Results_Meet_All_Sales_view, name='FarSeer_Results_Meet_All_Sales'),
    path('Existing_Projects/Data_Portal/FarSeer_Portal/Results_Summary_Meet_Forced_Sales/<str:project_id>/<str:quarry_id>/', views.FarSeer_Results_Meet_Forced_Sales_view, name='FarSeer_Results_Meet_Forced_Sales'),
    path('Existing_Projects/Data_Portal/FarSeer_Portal/Results_Summary_MaxGM/<str:project_id>/<str:quarry_id>/', views.FarSeer_Results_MaxGM_view, name='FarSeer_Results_MaxGM'),

    # Existing Projects / Data Portal / Network Optimisation
    path('Existing_Projects/Data_Portal/Network_Optimisation_Portal/<str:project_id>/', views.Network_Optimisation_Portal_view, name='Network_Optimisation_Portal'),
    path('Existing_Projects/Data_Portal/Network_Optimisation_Portal/Delete_Scenario/<str:project_id>/<str:scenario_id>/', views.Network_Optimisation_Clear_Scenario, name='Network_Optimisation_Clear_Scenario'),

    path('Existing_Projects/Data_Portal/Network_Optimisation_Baseline_Information/<str:project_id>/', views.NO_Baseline_Information_view, name='NO_Baseline_Information'),
    path('Existing_Projects/Data_Portal/Network_Optimisation_Baseline_Information_bridge/<str:project_id>/', views.NO_Baseline_Information_to_Results_bridge, name='NO_Baseline_Information_to_Results'),
    path('Existing_Projects/Data_Portal/Network_Optimisation_Baseline_Results/<str:project_id>/', views.NO_Baseline_Results_view, name='NO_Baseline_Results'),

    path('Existing_Projects/Data_Portal/Network_Optimisation_Scenario_Information/<str:project_id>/<str:scenario_id>/', views.NO_Scenario_Information_view, name='NO_Scenario_Information'),
    path('Existing_Projects/Data_Portal/Network_Optimisation_Scenario_Information_bridge/<str:project_id>/<str:scenario_id>/', views.NO_Scenario_Information_to_Results_bridge, name='NO_Scenario_Information_to_Results'),
    path('Existing_Projects/Data_Portal/Network_Optimisation_Scenario_Results/<str:project_id>/<str:scenario_id>/', views.NO_Scenario_Results_view, name='NO_Scenario_Results'),


]