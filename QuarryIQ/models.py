import os
from django.db import models
from django.contrib.auth.models import User
from uuid import uuid4
import uuid
import shutil


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ProjectData(models.Model):
    project_id = models.CharField(max_length=50, unique=True)
    project_company = models.CharField(max_length=50, default='')
    project_name = models.CharField(max_length=50)
    project_time_period = models.CharField(max_length=20)
    currency = models.CharField(max_length=10)
    EXWLocation = models.BooleanField(default=False)
    filtered_df_json = models.TextField(default='{}')
    distances_df_json = models.TextField(default='{}')
    modified_transactions_df_json = models.TextField(default='{}')
    modified_transactions_day_df_json = models.TextField(default='{}')
    quarries = models.ManyToManyField('Quarry')
    # Network Optimisation
    baseline_infoTable_df_json = models.TextField(default='{}')
    radiusTable_df_json = models.TextField(default='{}')
    transport_cost_df_json = models.TextField(default='{}')
    reduced_customers_df_json = models.TextField(default='{}')
    small_customers_df_json = models.TextField(default='{}')
    NO_Baseline_Raw_Results_df_json = models.TextField(default='{}')
    Transport_Cost_per_Customer_df_json = models.TextField(default='{}')
    NO_Baseline_Customer_Results_df_json = models.TextField(default='{}')
    NO_Baseline_Gross_Sales = models.IntegerField(default=0)
    NO_Baseline_Done = models.BooleanField(default=False)


    def save(self, *args, **kwargs):
        # Generate a unique project ID only if it's a new instance
        if not self.pk:
            self.project_id = uuid4().hex
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        # Delete the associated folder
        shutil.rmtree(self.Existing_Projects_Data_Portal_path(), ignore_errors=True)
        super().delete(*args, **kwargs)

    def Existing_Projects_Data_Portal_path(self):
        return os.path.join(BASE_DIR, 'QuarryIQ', 'templates', 'QuarryIQ', 'Project_Data', self.project_id)
    def Heatmap_folder_path(self):
        return os.path.join(BASE_DIR, 'QuarryIQ', 'templates', 'QuarryIQ', 'Project_Data', self.project_id, 'Heatmaps')
    def Streamsmap_folder_path(self):
        return os.path.join(BASE_DIR, 'QuarryIQ', 'templates', 'QuarryIQ', 'Project_Data', self.project_id, 'Streamsmaps')

class Quarry(models.Model):
    quarry_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=100, default='')
    year = models.CharField(max_length=25, default='')
    # P4P
    clusterToBox_df_json = models.TextField(default='{}')
    clusterToBox_df_change = models.BooleanField(default=False)
    productSalesVolume_df_json = models.TextField(default='{}')
    productAggLevy_df_json = models.TextField(default='{}')
    productProdVolume_df_json = models.TextField(default='{}')
    productProductCluster_df_json = models.TextField(default='{}')
    costBoxProdVolume_df_json = models.TextField(default='{}')
    productListFull_df_json = models.TextField(default='{}')
    PLAllocationData_df_json = models.TextField(default='{}')
    P4PFullSummary_df_json = models.TextField(default='{}')
    # B4C
    productComposition_df_json = models.TextField(default='{}')
    constituentModesBalance_df_json = models.TextField(default='{}')
    capacityModes_df_json = models.TextField(default='{}')
    # B4C Simulator
    salesSimulation_df_json = models.TextField(default='{}')
    minVC_Simulation_Table_df_json = models.TextField(default='{}')
    modes_Hours_minVC_Simulation_df_json = models.TextField(default='{}')
    maxGM_Simulation_Table_df_json = models.TextField(default='{}')
    modes_Hours_maxGM_Simulation_df_json = models.TextField(default='{}')
    # FarSeer
    startingMonth = models.CharField(max_length=50, default='')
    finalMonth = models.CharField(max_length=50, default='')
    FarSeer_stockInfo_df_json = models.TextField(default='{}')
    maxOpeningHours_df_json = models.TextField(default='{}')
    salesForecast_df_json = models.TextField(default='{}')
    FarSeer_meetAllSales_results_df_json = models.TextField(default='{}')
    FarSeer_meetAllSales_hours_df_json = models.TextField(default='{}')
    FarSeer_meetForcedSales_results_df_json = models.TextField(default='{}')
    FarSeer_meetForcedSales_hours_df_json = models.TextField(default='{}')
    FarSeer_maxGM_results_df_json = models.TextField(default='{}')
    FarSeer_maxGM_hours_df_json = models.TextField(default='{}')
    FarSeer_MeetAllSales_OptimisationSuccess = models.BooleanField(default=True)
    FarSeer_MeetForcedSales_OptimisationSuccess = models.BooleanField(default=True)
    FarSeer_MaxGM_OptimisationSuccess = models.BooleanField(default=True)

    def __str__(self):
        return self.name


class Scenario(models.Model):
    OPTIMISATION_CHOICES = [
        ('meeting_all_sales', 'Meeting All Sales'),
        ('max_gross_margin', 'Maximizing Gross Margin'),
    ]

    scenario_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    project = models.ForeignKey(ProjectData, on_delete=models.CASCADE, related_name='scenarios')
    scenario_name = models.CharField(max_length=100)
    optimisation_type = models.CharField(max_length=20, choices=OPTIMISATION_CHOICES, default='meeting_all_sales')
    scenario_infoTable_df_json = models.TextField(default='{}')
    NO_Scenario_Raw_Results_df_json = models.TextField(default='{}')
    NO_Scenario_Customer_Results_df_json = models.TextField(default='{}')
    NO_Scenario_Done = models.BooleanField(default=False)


    def __str__(self):
        return self.scenario_name


class ProjectAccess(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    projects = models.ManyToManyField(ProjectData)

    def __str__(self):
        return f"{self.user.username}"



class UploadedGLPK(models.Model):
    original_file_name = models.CharField(max_length=255)
    encrypted_content = models.BinaryField(default=b'')  # Store only encrypted content
    upload_date = models.DateTimeField(auto_now_add=True)
    uploaded_file_name = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.original_file_name