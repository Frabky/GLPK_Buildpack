from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from . import Sales_Data_Transformations
from .P4P_Results import P4P_Cost_Box_Table, P4P_Cost_Box_Cluster_Table, P4P_Products_Table, P4P_Sales_Margin_Vizualisation, P4P_Negative_Margin_Table, P4P_Download_Data
from . import B4C_Results
from . import B4C_Simulator
from . import FarSeer
from . import Portal_Completion
from . import Network_Optimisation
import os
import json
import pandas as pd
from io import StringIO
from .models import ProjectData, Quarry, ProjectAccess, Scenario
from django.core.files.storage import FileSystemStorage
from django.db import transaction
import uuid
from django.core.files.base import ContentFile
import calendar
from django.http import HttpResponseRedirect
from django.contrib.admin.views.decorators import user_passes_test
from django.contrib.auth.models import User, Group
from django.db.models import Prefetch
from django.contrib.auth import update_session_auth_hash
from django.contrib import messages
from django.contrib.auth.forms import PasswordChangeForm
import math
from django.http import HttpResponse
from django.db import IntegrityError
from django.conf import settings
from django.core.mail import send_mail
from .forms import ContactForm
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from .models import UploadedGLPK
from .utils import encrypt_data, get_mod_file_content
import tempfile


def format_with_commas(num):
    return "{:,.0f}".format(num)

@csrf_exempt
def csp_report(request):
    if request.method == 'POST':
        try:
            report = json.loads(request.body)
            # Log or process the CSP report
            print('CSP Report:', report)  # Example: printing to the console
            return HttpResponse(status=204)  # No Content
        except json.JSONDecodeError:
            return HttpResponse(status=400)  # Bad Request if JSON is malformed
    return HttpResponse(status=405)  # Method Not Allowed for non-POST requests




# === Sagitta Nova Website ===
def Home(request):
    return render(request, 'QuarryIQ/Home.html')


def Contact(request):
    if request.method == 'POST':
        form = ContactForm(request.POST)
        if form.is_valid():
            name = form.cleaned_data['name']
            email = form.cleaned_data['email']
            message = form.cleaned_data['message']

            # Fixed subject line
            subject = "Contact Form Submission"

            # Compose the email body to include the sender's name
            email_body = f"""
            You have a new message from {name} ({email}):

            {message}
            """

            # Send the email
            send_mail(
                subject,  # Subject of the email
                email_body,  # Message body
                email,  # From email
                [settings.DEFAULT_FROM_EMAIL],  # To email
                fail_silently=False,
            )

            # Redirect or display a success message
            messages.success(request, 'Your message has been sent successfully.')
            return redirect('Contact')
    else:
        form = ContactForm()

    return render(request, 'QuarryIQ/Contact.html', {'form': form})





@login_required
def User_Profile_view(request):
    error_message = None
    success_message = None

    if request.method == 'POST':
        form = PasswordChangeForm(request.user, request.POST)
        if form.is_valid():
            user = form.save()
            update_session_auth_hash(request, user)  # Keep the user logged in
            success_message = 'Your password was successfully updated!'
        else:
            # Check specific errors and set appropriate error messages
            new_password1 = form.cleaned_data.get('new_password1')
            new_password2 = request.POST.get('new_password2')

            if 'old_password' in form.errors:
                error_message = 'Incorrect current password. Password has not been updated.'
            elif new_password2 != new_password1:
                error_message = 'The new passwords do not match. Password has not been updated.'
            elif 'new_password2' in form.errors:
                error_message = 'New password not meeting security requirements:<ul>'
                error_message += '<li>Minimum length must be 8 characters.</li>'
                error_message += '<li>Password cannot be entirely numbers.</li>'
                error_message += '<li>Password cannot be too common.</li>'
                error_message += '<li>Password cannot contain email address.</li>'
                error_message += '</ul>Password has not been updated'
            else:
                error_message = 'There was an error updating your password. Password has not been updated.'
            messages.error(request, error_message)
    else:
        form = PasswordChangeForm(request.user)

    # Retrieve user information
    user_email = request.user.email
    user_first_name = request.user.first_name
    user_last_name = request.user.last_name

    # Pass user information to the template context
    context = {
        'user_email': user_email,
        'user_first_name': user_first_name,
        'user_last_name': user_last_name,
        'form': form,
        'error_message': error_message,
        'success_message': success_message,
    }

    # Clear messages from the session after they have been displayed to avoid seeing them in /admin
    storage = messages.get_messages(request)
    storage.used = True

    return render(request, 'QuarryIQ/User_Profile.html', context)



@user_passes_test(lambda u: u.is_superuser)
def Admin_Portal_view(request):
    if request.method == 'POST':
        # Get the selected project ID
        selected_project_id = request.POST.get('project_id')

        # Get the selected user IDs
        selected_user_ids = request.POST.getlist('user_ids')

        # Check if there are selected users and projects
        if selected_project_id and selected_user_ids:
            # Associate selected users with the selected project
            for user_id in selected_user_ids:
                user = User.objects.get(pk=user_id)
                project = ProjectData.objects.get(project_id=selected_project_id)

                # Check if the association already exists
                if not ProjectAccess.objects.filter(user=user, projects=project).exists():
                    project_access = ProjectAccess.objects.create(user=user)
                    project_access.projects.add(project)
        else:
            # Return a bad request response if no project or user is selected
            return redirect('Admin_Portal')

    # Retrieve all projects
    projects = ProjectData.objects.all()

    # Iterate over all projects
    for project in projects:
        filtered_df_json = project.filtered_df_json
        filtered_df = pd.read_json(StringIO(filtered_df_json))

        if filtered_df.empty:  # Check if project has incomplete data
            with transaction.atomic():
                project.delete()

    # Retrieve all projects
    projects = ProjectData.objects.all().order_by('project_company', 'project_name', 'project_time_period')

    # Retrieve all users with their associated groups
    users_with_groups = []
    for user in User.objects.all():
        user_groups = user.groups.all()
        users_with_groups.append({'user': user, 'groups': user_groups})

    # Sort the list primarily by the first group name and secondarily by username
    users_with_groups = sorted(users_with_groups, key=lambda x: (x['groups'][0].name if x['groups'] else '', x['user'].username))

    # Retrieve all project-user associations with related projects prefetched
    project_user_associations = ProjectAccess.objects.filter(
        user__in=User.objects.all()
    ).prefetch_related(
        Prefetch('projects', queryset=ProjectData.objects.all())
    )

    context = {
        'projects': projects,
        'users_with_groups': users_with_groups,
        'project_user_associations': project_user_associations,
    }

    return render(request, 'QuarryIQ/Admin_Portal.html', context)


@user_passes_test(lambda u: u.is_superuser)
def Model_Portal_view(request):
    # Dictionary to store file names, last upload dates, and uploaded file names
    file_info = {
        'B4C_minVC_Base': {'file_name': 'B4C MinVC Base', 'upload_date': None, 'uploaded_file_name': None},
        'B4C_maxGM_Base': {'file_name': 'B4C MaxGM Base', 'upload_date': None, 'uploaded_file_name': None},
        'B4C_minVC_Simulation': {'file_name': 'B4C MinVC Simulation', 'upload_date': None, 'uploaded_file_name': None},
        'B4C_maxGM_Simulation': {'file_name': 'B4C MaxGM Simulation', 'upload_date': None, 'uploaded_file_name': None},
        'FarSeer_meetAllSales': {'file_name': 'FarSeer Meet All Sales', 'upload_date': None, 'uploaded_file_name': None},
        'FarSeer_meetForcedSales': {'file_name': 'FarSeer Meet Forced Sales', 'upload_date': None, 'uploaded_file_name': None},
        'FarSeer_maxGM': {'file_name': 'FarSeer MaxGM', 'upload_date': None, 'uploaded_file_name': None},
        'Network_Optimisation_Baseline': {'file_name': 'Network Optimisation Baseline', 'upload_date': None, 'uploaded_file_name': None},
        'Network_Optimisation_Scenario_MeetingAllSales': {'file_name': 'Network Optimisation Scenario Meeting All Sales', 'upload_date': None, 'uploaded_file_name': None},
        'Network_Optimisation_Scenario_MaximizingGrossMargin': {'file_name': 'Network Optimisation Scenario Maximizing Gross Margin', 'upload_date': None, 'uploaded_file_name': None},
    }

    if request.method == 'POST':
        for field in file_info.keys():
            if field in request.FILES:
                uploaded_file = request.FILES[field]
                content = uploaded_file.read().decode('utf-8')

                # Encrypt the file content
                encrypted_content = encrypt_data(content)

                # Check if an entry already exists and update it
                existing_entry = UploadedGLPK.objects.filter(original_file_name=field).first()
                if existing_entry:
                    existing_entry.encrypted_content = encrypted_content
                    existing_entry.upload_date = timezone.now()
                    existing_entry.uploaded_file_name = uploaded_file.name
                    existing_entry.save()
                else:
                    # Save the encrypted content and the actual file name to the database
                    UploadedGLPK.objects.create(
                        original_file_name=field,
                        encrypted_content=encrypted_content,  # Save the encrypted content
                        upload_date=timezone.now(),
                        uploaded_file_name=uploaded_file.name  # Save the actual file name
                    )

        # Update the last upload timestamps and uploaded file names
        for field in file_info.keys():
            latest_upload = UploadedGLPK.objects.filter(original_file_name=field).order_by('-upload_date').first()
            if latest_upload:
                file_info[field]['upload_date'] = latest_upload.upload_date
                file_info[field]['uploaded_file_name'] = latest_upload.uploaded_file_name

        # Redirect after POST to avoid resubmission
        return redirect('Model_Portal')

    else:
        # Update the last upload timestamps and uploaded file names when handling GET request
        for field in file_info.keys():
            latest_upload = UploadedGLPK.objects.filter(original_file_name=field).order_by('-upload_date').first()
            if latest_upload:
                file_info[field]['upload_date'] = latest_upload.upload_date
                file_info[field]['uploaded_file_name'] = latest_upload.uploaded_file_name

    # Pass the context to the template
    context = {
        'file_info': file_info,
    }

    return render(request, 'QuarryIQ/Model_Portal.html', context)



@user_passes_test(lambda u: u.is_superuser)
def Flush_uploaded_glpk(request):
    if request.method == 'POST':
        UploadedGLPK.objects.all().delete()  # Delete all records in the UploadedGLPK model
    return redirect('Model_Portal')  # Redirect back to the Model Portal page


@user_passes_test(lambda u: u.is_superuser)
def Delete_Access_bridge(request, association_id):
    # Get the ProjectAccess instance to delete
    association = get_object_or_404(ProjectAccess, pk=association_id)

    if request.method == 'POST':
        # Delete the ProjectAccess instance
        association.delete()
        # Redirect to a success page or another appropriate URL
        return redirect('Admin_Portal')

    return redirect('Admin_Portal')


@user_passes_test(lambda u: u.is_superuser)
def Delete_Project_bridge(request, project_id):
    if request.method == 'POST':
        # Retrieve project data or return a 404 error if not found
        project_data = get_object_or_404(ProjectData, project_id=project_id)

        # Delete the project
        project_data.delete()

        # Redirect back to Admin_Portal or any appropriate page
        return redirect('Admin_Portal')
    else:
        # Return a bad request response if request method is not POST
        return redirect('Admin_Portal')


@user_passes_test(lambda u: u.is_superuser)
def New_Project_General_Info_view(request):
    # Create a new project instance
    project = ProjectData.objects.create()

    # Get the project_id of the newly created project
    project_id = project.project_id

    # Get all user groups except 'Superuser' and 'Editors'
    user_groups = Group.objects.exclude(name__in=["Superuser", "Editors"])

    context = {
        'project_id': project_id,
        'user_groups': user_groups,
    }

    return render(request, 'QuarryIQ/New_Project/NewProject_General_Info.html', context)


@user_passes_test(lambda u: u.is_superuser)
def General_Info_to_Data_Cleaning_bridge(request, project_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            project = get_object_or_404(ProjectData, project_id=project_id)

            # Load tableData from request body
            data = json.loads(request.body)

            # Extract data from the request body
            project_company = data.get('projectCompany', '')  # Extract project name
            project_name = data.get('projectName', '')  # Extract project name
            time_period = data.get('timePeriod', '')  # Extract time period
            currency = data.get('currency', '')  # Extract currency
            EXWLocation = data.get('EXWLocation', '')  # Extract currency

            if EXWLocation == "Yes":
                EXWLocation = True
            else:
                EXWLocation = False

            project.project_company = project_company
            project.project_name = project_name
            project.project_time_period = time_period
            project.currency = currency
            project.EXWLocation = EXWLocation

            # Save the new project
            project.save()

            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            # Redirect the user to a specific error page for malformed JSON
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)


@user_passes_test(lambda u: u.is_superuser)
def Upload_Data(request, project_id):
    # Retrieve project_company
    project = get_object_or_404(ProjectData, project_id=project_id)
    project_company = project.project_company

    if request.method == 'POST':
        # Retrieve the uploaded files
        filtered_file = request.FILES.get('filteredFile')
        distances_file = request.FILES.get('distancesFile')

        filtered_expected_columns = ['Day', 'Quarries', 'Incoterms', 'Location', 'Product', 'Sales_Volume', 'Revenue', 'Customers', 'Customers location', 'Quarries coordinates', 'Customers location coordinates']
        distances_expected_columns = ['Quarries', 'Customers', 'Distance']


        # Process the filtered file
        if filtered_file:
            # Load the Excel file into a pandas DataFrame
            filtered_df = pd.read_excel(filtered_file)
            if not all(col in filtered_df.columns for col in filtered_expected_columns):
                filtered_file_error = "Filtered file not matching columns pattern. Columns should be 'Day', 'Quarries', 'Incoterms', 'Location', 'Product', 'Sales_Volume', 'Revenue', 'Customers', 'Customers location', 'Quarries coordinates', 'Customers location coordinates'."

                context = {
                    'project_id': project_id,
                    'project_company': project_company,
                    'project_name': project.project_name,
                    'project_time_period': project.project_time_period,
                    'filtered_file_error': filtered_file_error,
                }

                return render(request, 'QuarryIQ/New_Project/NewProject_Upload_Data.html', context)

            # Save filtered_df
            filtered_df_json = filtered_df.to_json(orient='records')
            project.filtered_df_json = filtered_df_json
            project.save()


        # Process the distances file
        if distances_file:
            # Load the Excel file into a pandas DataFrame
            distances_df = pd.read_excel(distances_file)
            if not all(col in distances_df.columns for col in distances_expected_columns):
                distances_df_error = "Distances file not matching columns pattern. Columns should be 'Quarries', 'Customers', 'Distance'."

                context = {
                    'project_id': project_id,
                    'project_company': project_company,
                    'project_name': project.project_name,
                    'project_time_period': project.project_time_period,
                    'distances_df_error': distances_df_error,
                }

                return render(request, 'QuarryIQ/New_Project/NewProject_Upload_Data.html', context)

            # Save distances_df
            distances_df_json = distances_df.to_json(orient='records')
            project.distances_df_json = distances_df_json
            project.save()

        return redirect('Admin_Portal')

    context = {
        'project_id': project_id,
        'project_company': project_company,
        'project_name': project.project_name ,
        'project_time_period': project.project_time_period,
    }

    return render(request, 'QuarryIQ/New_Project/NewProject_Upload_Data.html', context)



@login_required
def Project_List_view(request):
    # Retrieve projects where the current user has been granted access through ProjectAccess
    user_projects = ProjectData.objects.filter(projectaccess__user=request.user).order_by(
        'project_company', 'project_name', 'project_time_period', 'currency'
    )

    is_editor = request.user.groups.filter(name='Editors').exists()

    context = {
        'user_projects': user_projects,
        'is_editor': is_editor,  # Pass the boolean to the template context
    }


    return render(request, 'QuarryIQ/Project_List.html', context)


@login_required
def Modify_Information_view(request, project_id):
    # Retrieve project data or return a 404 error if not found
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    context = {
        'project_id': project_id,
        'project_company': project_data.project_company,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
    }

    return render(request, 'QuarryIQ/Modify_Project_Information.html', context)



@login_required
def Data_Portal_view(request, project_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    # Retrieve modified_transactions_df and NO_Baseline_Done from project_data
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')
    NO_Baseline_Done = project_data.NO_Baseline_Done

    P4P_completion = None
    B4C_completion = None
    FarSeer_completion = None

    if modified_transactions_df.empty:
        modified_transactions_df_empty = True
    else:
        modified_transactions_df_empty = False
        P4P_completion = Portal_Completion.P4P_Completion(project_data)
        B4C_completion = Portal_Completion.B4C_Completion(project_data)
        FarSeer_completion = Portal_Completion.FarSeer_Completion(project_data)

    is_editor = request.user.groups.filter(name='Editors').exists()

    context = {
        'project_id': project_id,
        'project_data': project_data,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'project_company': project_data.project_company,
        'modified_transactions_df_empty': modified_transactions_df_empty,
        'P4P_completion': P4P_completion,
        'B4C_completion': B4C_completion,
        'FarSeer_completion': FarSeer_completion,
        'is_editor': is_editor,  # Pass the boolean to the template context
        'NO_Baseline_Done': NO_Baseline_Done,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Data_Portal.html', context)




@login_required
def Product_Cleaning_view(request, project_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    # Retrieve filtered_df from project_data
    filtered_df_json = project_data.filtered_df_json
    filtered_df = pd.read_json(StringIO(filtered_df_json), orient='records')

    products_df = Sales_Data_Transformations.Products_extraction(filtered_df)
    products_list = products_df.to_dict(orient='records')
    # Convert products_list to JSON string
    products_json = json.dumps(products_list)

    context = {
        'project_id': project_id,
        'products_json': products_json,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Product_Cleaning/Product_Cleaning.html', context)


@login_required
def Handle_modified_products_view(request, project_id):
    if request.method == 'POST':
        # Retrieve project data or return a 404 error if not found
        project_data = get_object_or_404(ProjectData, project_id=project_id)

        # Retrieve modified_transactions_df from project_data
        modified_transactions_df_json = project_data.modified_transactions_df_json
        modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')
        modified_transactions_df = pd.DataFrame()

        # Save modified_transactions_df and delete all existing quarries
        project_data.modified_transactions_df_json = modified_transactions_df.to_json(orient='records')
        project_data.quarries.clear()
        project_data.save()

        # Load modifiedProducts from request body
        modifiedProducts = json.loads(request.body)

        # Retrieve modified_transactions_df from project_data
        filtered_df_json = project_data.filtered_df_json
        filtered_df = pd.read_json(StringIO(filtered_df_json), orient='records')

        # Create modified_transactions_df and transform it to json format
        modified_transactions_df, modified_transactions_day_df = Sales_Data_Transformations.Modified_products_integration(filtered_df, modifiedProducts)
        modified_transactions_df_json = modified_transactions_df.to_json(orient='records')
        modified_transactions_day_df_json = modified_transactions_day_df.to_json(orient='records')

        # Create quarries
        quarry_names = modified_transactions_df['Quarries'].unique()
        with transaction.atomic():
            try:
                # Create quarries and associate them with the project
                for quarry_name in quarry_names:
                    quarry_uuid = uuid.uuid4()
                    quarry, created = Quarry.objects.get_or_create(quarry_id=quarry_uuid, name=quarry_name)
                    project_data.quarries.add(quarry)

                # Save the ProjectData instance
                project_data.save()


            except Exception as e:
                # Rollback the transaction if an error occurs
                transaction.rollback()
                return JsonResponse({'error': str(e)}, status=500)

        # Save modified_transactions_df & modified_transactions_day_df
        project_data.modified_transactions_df_json = modified_transactions_df_json
        project_data.modified_transactions_day_df_json = modified_transactions_day_df_json
        project_data.save()

        # Add a response for the POST request
        return JsonResponse({'success': True})

    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)






# Existing Projects / Data Portal / Heatmaps (3 views)
@login_required
def Heatmap_Portal_view(request, project_id, product_group=None):
    # Retrieve project data based on project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    # Accessing modified_transactions_df from project_data
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Find unique_product_groups
    unique_product_groups = modified_transactions_df['Product Group'].unique()

    # Calculations related to volumes
    total_volume = int(modified_transactions_df['Sales_Volume'].sum())

    # Initialize an empty list to store data for each product group
    data = []

    # Iterate over unique product groups
    for product_group in unique_product_groups:
        # Filter transactions for the current product group
        group_transactions = modified_transactions_df[modified_transactions_df['Product Group'] == product_group]
        # Calculate total sales volume for the product group
        group_volume = group_transactions['Sales_Volume'].sum()
        # Calculate group percentage
        group_percentage = (group_volume / total_volume * 100).round(2)
        # Append the product group and total sales volume to the DataFrame
        data.append({'Product_Group': product_group, 'Sales_Volume': group_volume, 'Percentage': group_percentage})

    # Convert the list of dictionaries to a DataFrame
    product_group_sales_df = pd.DataFrame(data)
    # Sort the DataFrame by 'Sales Volume' column in descending order
    product_group_sales_df = product_group_sales_df.sort_values(by='Sales_Volume', ascending=False)
    # Format
    product_group_sales_df['Sales_Volume'] = product_group_sales_df['Sales_Volume'].apply(format_with_commas)

    total_volume = format_with_commas(total_volume)

    context = {
        'unique_product_groups': unique_product_groups,
        'total_volume': total_volume,
        'project_id': project_id,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'product_group_sales_df': product_group_sales_df,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Heatmaps/Heatmap_Portal.html', context)


@login_required
def Heatmap_all_product_groups_view(request, project_id):
    # Retrieve project data based on project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Get the heatmap folder path for the project
    template_folder = project_data.Heatmap_folder_path()

    # Accessing modified_transactions_df from project_data
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Create a folder to store Heatmap HTML files in the templates directory if it doesn't exist
    os.makedirs(template_folder, exist_ok=True)

    # Use Django's FileSystemStorage for managing files
    fs = FileSystemStorage(location=template_folder)

    # Check if Heatmap_all.html already exists in the folder
    if not fs.exists('Heatmap_all.html'):
        # Call the Heatmap function to generate heatmap HTML
        heatmaps = Sales_Data_Transformations.Heatmap(modified_transactions_df)

        # Get the heatmap for all product groups combined
        Heatmap_all_product_groups = heatmaps['All_Product_Groups']

        # Save the map to an HTML file with explicit encoding
        Heatmap_all_product_groups_html = Heatmap_all_product_groups.get_root().render().encode('utf-8')
        fs.save('Heatmap_all.html', ContentFile(Heatmap_all_product_groups_html))


    context = {
        'project_id': project_id,
        'project_name':project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'template_folder': template_folder,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Heatmaps/Heatmap_all_product_groups.html', context)

@login_required
def Heatmap_product_group_view(request, project_id, product_group=None):
    # Retrieve project data based on project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Get the heatmap folder path for the project
    template_folder = project_data.Heatmap_folder_path()

    # Retrieve modified_transaction_df
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Use Django's FileSystemStorage for managing files
    fs = FileSystemStorage(location=template_folder)

    # Check if Heatmaps for individual product groups already exist in the folder
    if not fs.exists(f'Heatmap_{product_group}.html'):
        # Call the Heatmap function to generate heatmap HTML
        heatmaps = Sales_Data_Transformations.Heatmap(modified_transactions_df)

        # Save individual heatmap HTML files with product group name
        for pg, heatmap in heatmaps.items():
            Heatmap_product_group_html = heatmap.get_root().render().encode('utf-8')
            fs.save(f'Heatmap_{pg}.html', ContentFile(Heatmap_product_group_html))

    # Filter the DataFrame for the selected incoterm
    selected_product_group_df = modified_transactions_df[modified_transactions_df['Product Group'] == product_group]

    # Calculate the volume for the selected product_group
    selected_product_group_volume = int(selected_product_group_df['Sales_Volume'].sum())
    selected_product_group_volume_percentage = round(100 * (selected_product_group_df['Sales_Volume'].sum() / modified_transactions_df['Sales_Volume'].sum()), 1)

    # Pass product group volumes to the template
    context = {
        'selected_product_group_volume': selected_product_group_volume,
        'selected_product_group_volume_percentage': selected_product_group_volume_percentage,
        'product_group': product_group,
        'project_id': project_id,
        'template_folder': template_folder,
    }

    # Render the template
    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Heatmaps/Heatmap_product_group.html', context)




# Existing Projects / Data Portal / Streamsmaps (3 views)
@login_required
def Streamsmap_Portal_view(request, project_id):
    # Retrieve project data based on project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    # Accessing modified_transactions_df from project_data
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Find unique_product_groups
    unique_incoterms = modified_transactions_df['Incoterms'].unique()

    # Calculate totals
    total_volume = int(modified_transactions_df['Sales_Volume'].sum())
    unique_customers = modified_transactions_df['Customers'].nunique()
    mean_per_customer = modified_transactions_df['Sales_Volume'].mean()

    # Initialize an empty list to store data for each product group
    data = []

    # Iterate over unique product groups
    for incoterm in unique_incoterms:
        # Filter transactions for the current product group
        group_transactions = modified_transactions_df[modified_transactions_df['Incoterms'] == incoterm]
        # Calculate total sales volume for the product group
        group_volume = group_transactions['Sales_Volume'].sum()
        # Calculate group percentage
        group_percentage = (group_volume / total_volume * 100).round(2)
        # Calculate the unique customers
        group_customer = group_transactions['Customers'].nunique()
        # Calculate volume mean per customer
        group_mean_volume = group_transactions['Sales_Volume'].mean()
        # Append the product group and total sales volume to the DataFrame
        data.append({'Incoterm': incoterm, 'Sales_Volume': group_volume, 'Percentage': group_percentage, 'Unique_Customers': group_customer, 'Mean_Volume': group_mean_volume})

    # Convert the list of dictionaries to a DataFrame
    incoterm_sales_df = pd.DataFrame(data)
    # Sort the DataFrame by 'Sales Volume' column in descending order
    incoterm_sales_df = incoterm_sales_df.sort_values(by='Sales_Volume', ascending=False)
    # Format
    incoterm_sales_df['Sales_Volume'] = incoterm_sales_df['Sales_Volume'].apply(format_with_commas)
    incoterm_sales_df['Mean_Volume'] = incoterm_sales_df['Mean_Volume'].apply(format_with_commas)
    incoterm_sales_df['Unique_Customers'] = incoterm_sales_df['Unique_Customers'].apply(format_with_commas)

    # Format totals
    total_volume = format_with_commas(total_volume)
    unique_customers = format_with_commas(unique_customers)
    mean_per_customer = format_with_commas(mean_per_customer)


    context = {
        'unique_incoterms': unique_incoterms,
        'total_volume': total_volume,
        'project_id': project_id,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'incoterm_sales_df': incoterm_sales_df,
        'unique_customers': unique_customers,
        'mean_per_customer': mean_per_customer,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Streamsmaps/Streamsmap_Portal.html', context)
@login_required
def Streamsmap_baseline_all_incoterms_view(request, project_id):
    # Retrieve project data based on project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Get the heatmap folder path for the project
    template_folder = project_data.Streamsmap_folder_path()

    # Accessing modified_transactions_df from project_data
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Create a folder to store Streamsmaps HTML files in the templates directory if it doesn't exist
    os.makedirs(template_folder, exist_ok=True)

    # Use Django's FileSystemStorage for managing files
    fs = FileSystemStorage(location=template_folder)

    # Check if Streamsmap already exists in the folder
    if not fs.exists('Streamsmap_baseline_all_incoterms.html'):
        # Call the Heatmap function to generate heatmap HTML
        Streams_map = Sales_Data_Transformations.Streamsmap_all_incoterms(modified_transactions_df)

        # Save the map to an HTML file with explicit encoding
        Streamsmap_all_incoterms_html = Streams_map.get_root().render().encode('utf-8')
        fs.save('Streamsmap_baseline_all_incoterms.html', ContentFile(Streamsmap_all_incoterms_html))

    # Calculations related to volumes
    all_incoterms_volume = int(modified_transactions_df['Sales_Volume'].sum())
    all_incoterms_count = modified_transactions_df['Customers'].nunique()
    all_incoterms_mean = int(modified_transactions_df['Sales_Volume'].mean())

    # Find unique_incoterms
    unique_incoterms = modified_transactions_df['Incoterms'].unique()

    context = {
        'unique_incoterms': unique_incoterms,
        'all_incoterms_volume': all_incoterms_volume,
        'all_incoterms_count': all_incoterms_count,
        'all_incoterms_mean': all_incoterms_mean,
        'project_id': project_id,
        'template_folder': template_folder,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Streamsmaps/Streams_maps_baseline_all_incoterms.html', context)

@login_required
def Streams_maps_baseline_incoterm_view(request, project_id, incoterm=None):
    # Retrieve project data based on project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Get the heatmap folder path for the project
    template_folder = project_data.Streamsmap_folder_path()

    # Retrieve modified_transaction_df
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Use Django's FileSystemStorage for managing files
    fs = FileSystemStorage(location=template_folder)

    # Check if the maps already exist before generating them
    if not fs.exists(f'Streamsmap_baseline_{incoterm}.html'):
        # Call Streamsmap_incoterm function to generate maps for all unique incoterms
        streams_maps = Sales_Data_Transformations.Streamsmap_incoterm(modified_transactions_df, incoterms=[incoterm])

        # Save the map to an HTML file
        streams_map = streams_maps[incoterm]
        streamsmap_html = streams_map.get_root().render().encode('utf-8')
        fs.save(f'Streamsmap_baseline_{incoterm}.html', ContentFile(streamsmap_html))

    context = {
        'incoterm': incoterm,
        'project_id': project_id,
        'template_folder': template_folder,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Streamsmaps/Streams_map_baseline_incoterm.html', context)



# Existing Projects / Data Portal / P4P (9 views)
@login_required
def P4P_Portal_view(request, project_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

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

    is_editor = request.user.groups.filter(name='Editors').exists()

    context = {
        'project_id': project_id,
        'project_data': project_data,
        'quarries_list': quarries_list,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'P4P_empty_df': P4P_empty_df,
        'is_editor': is_editor,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/P4P_Portal/P4P_Portal.html', context)

@login_required
def P4P_Cluster_to_box_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve clusterToBox_df
    clusterToBox_df_json = quarry.clusterToBox_df_json
    clusterToBox_df = pd.read_json(StringIO(clusterToBox_df_json), orient='records')

    # Initialize uniqueCostBox and uniqueProductCluster as an empty list
    uniqueCostBox = []
    uniqueProductCluster = []

    # Check if clusterToBox_df is empty
    if not clusterToBox_df.empty:
        # Retrieve unique cost box and product cluster
        uniqueCostBox = clusterToBox_df['Cost_Box'].unique()
        uniqueProductCluster = clusterToBox_df['Product_Cluster'].unique()

        # Assuming clusterToBox_df is your DataFrame containing the data
        clusterToBox_dict = {}

        for product_cluster in uniqueProductCluster:
            clusterToBox_dict[product_cluster] = {}
            for cost_box in uniqueCostBox:
                value = clusterToBox_df[
                    (clusterToBox_df['Product_Cluster'] == product_cluster) & (
                                clusterToBox_df['Cost_Box'] == cost_box)][
                    'Value'].iloc[0]
                clusterToBox_dict[product_cluster][cost_box] = value if not pd.isna(value) else ''
    else:
        clusterToBox_dict = {}


    context = {
        'project_id': project_id,
        'project_data': project_data,
        'quarry': quarry,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'project_time_period': project_data.project_time_period,
        'clusterToBox_df': clusterToBox_df,
        'uniqueCostBox': uniqueCostBox,
        'uniqueProductCluster': uniqueProductCluster,
        'clusterToBox_dict': clusterToBox_dict,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/P4P_Portal/P4P_Cluster_to_box.html', context)

@login_required
def P4P_Cluster_to_box_to_Product_list_bridge(request, project_id, quarry_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            project_data = get_object_or_404(ProjectData, project_id=project_id)
            # Retrieve the quarry based on quarry_id
            quarry = get_object_or_404(Quarry, pk=quarry_id)

            # Load tableData from request body
            tableData = json.loads(request.body)

            # Convert the JSON object to a DataFrame and set the right column type
            new_clusterToBox_df = pd.DataFrame(tableData[1:],columns=tableData[0])  # Skip the first row as it contains headers
            new_clusterToBox_df['Product_Cluster'] = new_clusterToBox_df['Product_Cluster'].astype(str)
            new_clusterToBox_df['Value'] = new_clusterToBox_df['Value'].astype(int)

            # Retrieve existing clusterToBox_df
            clusterToBox_df_json = quarry.clusterToBox_df_json
            existing_clusterToBox_df = pd.read_json(StringIO(clusterToBox_df_json), orient='records')

            # Check if existing_clusterToBox_df is not empty
            if not existing_clusterToBox_df.empty:
                existing_clusterToBox_df['Product_Cluster'] = existing_clusterToBox_df['Product_Cluster'].astype(str)
                existing_clusterToBox_df['Value'] = existing_clusterToBox_df['Value'].astype(int)
            else:
                pass

            # Check if the new DataFrame is different from the existing one
            if not new_clusterToBox_df.equals(existing_clusterToBox_df):
                # Retrieve clusterToBox_df_change
                clusterToBox_df_change = quarry.clusterToBox_df_change

                # Assign an empty DataFrame to productProductCluster_df & assign clusterToBox_df_change to True
                clusterToBox_df_change = True

                # Update the productProductCluster_df_json field in the Quarry model
                quarry.clusterToBox_df_change = clusterToBox_df_change
                quarry.save()

                # Delete Network Optimisation Baseline and Scenarios
                # Retrieve baseline dataframes and clear them
                NO_Baseline_Done = project_data.NO_Baseline_Done
                project_data.NO_Baseline_Done = False
                project_data.save()

                # Check if scenarios exist for the project and delete them if any
                if Scenario.objects.filter(project=project_data).exists():
                    scenarios = Scenario.objects.filter(project=project_data)
                    for scenario in scenarios:
                        scenario.NO_Scenario_Raw_Results_df_json = '{}'
                        scenario.NO_Scenario_Customer_Results_df_json = '{}'
                        scenario.NO_Scenario_Done = False
                        scenario.save()


            quarry.clusterToBox_df_json = new_clusterToBox_df.to_json(orient='records')
            quarry.save()

            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            # Redirect the user to a specific error page for malformed JSON
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)

@login_required
def P4P_Product_list_view(request, project_id, quarry_id):
    # Retrieve project data or return a 404 error if not found
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve dataframes
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')
    clusterToBox_df_json = quarry.clusterToBox_df_json
    clusterToBox_df = pd.read_json(StringIO(clusterToBox_df_json), orient='records')
    productListFull_df_json = quarry.productListFull_df_json
    productListFull_df = pd.read_json(StringIO(productListFull_df_json), orient='records')
    clusterToBox_df_change = quarry.clusterToBox_df_change

    # Get the name of the quarry
    quarry_name = quarry.name
    # Select the needed columns from modified_transactions_df
    selected_columns_df = modified_transactions_df[["Quarries", "Product", "Sales_Volume"]]
    # Filter the "Quarries" column based on the name of the quarry
    selected_columns_filtered_df = selected_columns_df[selected_columns_df["Quarries"] == quarry_name]
    # Group by "Product" and aggregate the sum of volumes
    productSalesVolume_df = selected_columns_filtered_df.groupby("Product").agg(
        Sales_Volume=("Sales_Volume", lambda x: round(x.sum()))  # Sum the volumes and round to the nearest integer
    ).reset_index()
    # Sort the DataFrame by the "Volume" column in descending order
    productSalesVolume_df = productSalesVolume_df.sort_values(by="Sales_Volume", ascending=False)

    # Format
    productSalesVolume_df['Sales_Volume'] = productSalesVolume_df['Sales_Volume'].apply(format_with_commas)

    # Save
    quarry.productSalesVolume_df_json = productSalesVolume_df.to_json(orient='records')
    quarry.save()

    # Extract Product Cluster from cluster_to_box_df
    unique_product_clusters = clusterToBox_df['Product_Cluster'].unique()

    context = {
        'project_id': project_id,
        'project_data': project_data,
        'quarry': quarry,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'currency': project_data.currency,
        'project_time_period': project_data.project_time_period,
        'productSalesVolume_df': productSalesVolume_df,
        'unique_product_clusters': unique_product_clusters,
        'productListFull_df': productListFull_df,
        'clusterToBox_df_change': clusterToBox_df_change,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/P4P_Portal/P4P_Product_list.html',context)

@login_required
def P4P_Product_list_to_PL_allocation_bridge(request, project_id, quarry_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            project_data = get_object_or_404(ProjectData, project_id=project_id)
            # Retrieve the quarry based on quarry_id
            quarry = get_object_or_404(Quarry, pk=quarry_id)

            # Load json data from request body
            jsonData = json.loads(request.body)

            # Extract the five variables from jsonData
            productAggLevy = jsonData['productAggLevy']
            productProdVolume = jsonData['productProdVolume']
            productProductCluster = jsonData['productProductCluster']
            productClusterProdVolume = jsonData['productClusterProdVolume']
            productStocks = jsonData['productStocks']


            # Retrieve existing productAggLevy_df
            productAggLevy_df_json = quarry.productAggLevy_df_json
            existing_productAggLevy_df = pd.read_json(StringIO(productAggLevy_df_json), orient='records')
            if not existing_productAggLevy_df.empty:
                existing_productAggLevy_df['Aggregate_Levy'] = existing_productAggLevy_df['Aggregate_Levy'].astype(float)

            # Convert the JSON object to a DataFrame and set the right column type
            new_productAggLevy_df = pd.DataFrame(productAggLevy[1:], columns=productAggLevy[0])  # Skip the first row as it contains headers
            new_productAggLevy_df['Aggregate_Levy'] = new_productAggLevy_df['Aggregate_Levy'].astype(float)


            # Retrieve existing productProdVolume_df
            productProdVolume_df_json = quarry.productProdVolume_df_json
            existing_productProdVolume_df = pd.read_json(StringIO(productProdVolume_df_json), orient='records')
            if not existing_productProdVolume_df.empty:
                existing_productProdVolume_df['Production Volume'] = existing_productProdVolume_df['Production Volume'].astype(int)

            # Convert the JSON object to a DataFrame and set the right column type
            new_productProdVolume_df = pd.DataFrame(productProdVolume[1:], columns=productProdVolume[0])  # Skip the first row as it contains headers
            new_productProdVolume_df['Production Volume'] = new_productProdVolume_df['Production Volume'].str.replace(',', '')
            new_productProdVolume_df['Production Volume'] = new_productProdVolume_df['Production Volume'].astype(int)

            # Retrieve existing productProductCluster_df
            productProductCluster_df_json = quarry.productProductCluster_df_json
            existing_productProductCluster_df = pd.read_json(StringIO(productProductCluster_df_json), orient='records')
            if not existing_productProductCluster_df.empty:
                existing_productProductCluster_df['Product_Cluster'] = existing_productProductCluster_df['Product_Cluster'].astype(str)

            # Convert the JSON object to a DataFrame and set the right column type
            new_productProductCluster_df = pd.DataFrame(productProductCluster[1:], columns=productProductCluster[0])  # Skip the first row as it contains headers
            new_productProductCluster_df['Product_Cluster'] = new_productProductCluster_df['Product_Cluster'].astype(str)



            # Convert the JSON object to a DataFrame and set the right column type
            productClusterProdVolume_df = pd.DataFrame(productClusterProdVolume[1:], columns=productClusterProdVolume[
                0])  # Skip the first row as it contains headers
            productClusterProdVolume_df['Product_Cluster'] = productClusterProdVolume_df['Product_Cluster'].astype(str)
            productClusterProdVolume_df['Production Volume'] = productClusterProdVolume_df[
                'Production Volume'].str.replace(',', '')
            productClusterProdVolume_df['Production Volume'] = productClusterProdVolume_df['Production Volume'].astype(int)
            productClusterProdVolume_df = productClusterProdVolume_df.groupby('Product_Cluster')[
                'Production Volume'].sum().reset_index()

            # Convert the JSON object to a DataFrame and set the right column type
            productStocks_df = pd.DataFrame(productStocks[1:],
                                            columns=productStocks[0])  # Skip the first row as it contains headers
            productStocks_df['Opening_Stock'] = productStocks_df['Opening_Stock'].astype(float)
            productStocks_df['Closing_Stock'] = productStocks_df['Closing_Stock'].astype(float)

            # Retrieve clusterToBox_df & productSalesVolume_df
            clusterToBox_df_json = quarry.clusterToBox_df_json
            clusterToBox_df = pd.read_json(StringIO(clusterToBox_df_json), orient='records')
            clusterToBox_df['Product_Cluster'] = clusterToBox_df['Product_Cluster'].astype(str)
            productSalesVolume_df_json = quarry.productSalesVolume_df_json
            productSalesVolume_df = pd.read_json(StringIO(productSalesVolume_df_json), orient='records')

            # Merge both dataframes
            merged_df = pd.merge(productClusterProdVolume_df, clusterToBox_df, on='Product_Cluster', how='left')
            # Convert the 'Value' column to numeric type
            merged_df['Value'] = pd.to_numeric(merged_df['Value'])
            merged_df['Cost_Box_Prod_Volume'] = merged_df['Production Volume'] * (merged_df['Value'] / 100)

            # Retrieve existing costBoxProdVolume_df
            costBoxProdVolume_df_json = quarry.costBoxProdVolume_df_json
            existing_costBoxProdVolume_df = pd.read_json(StringIO(costBoxProdVolume_df_json), orient='records')
            if not existing_costBoxProdVolume_df.empty:
                existing_costBoxProdVolume_df['Cost_Box_Prod_Volume'] = existing_costBoxProdVolume_df['Cost_Box_Prod_Volume'].astype(int)

            # Get costBoxProdVolume_df
            new_costBoxProdVolume_df = merged_df.groupby('Cost_Box')['Cost_Box_Prod_Volume'].sum().reset_index()
            new_costBoxProdVolume_df['Cost_Box_Prod_Volume'] = new_costBoxProdVolume_df['Cost_Box_Prod_Volume'].astype(int)


            # Check if the new DataFrame is different from the existing one
            if (not new_productAggLevy_df.equals(existing_productAggLevy_df) or not new_productProdVolume_df.equals(existing_productProdVolume_df)
                    or not new_productProductCluster_df.equals(existing_productProductCluster_df) or not new_costBoxProdVolume_df.equals(existing_costBoxProdVolume_df)):
                # Delete Network Optimisation Baseline and Scenarios
                # Retrieve baseline dataframes and clear them
                NO_Baseline_Done = project_data.NO_Baseline_Done
                project_data.NO_Baseline_Done = False
                project_data.save()

                # Check if scenarios exist for the project and delete them if any
                if Scenario.objects.filter(project=project_data).exists():
                    scenarios = Scenario.objects.filter(project=project_data)
                    for scenario in scenarios:
                        scenario.NO_Scenario_Raw_Results_df_json = '{}'
                        scenario.NO_Scenario_Customer_Results_df_json = '{}'
                        scenario.NO_Scenario_Done = False
                        scenario.save()

            quarry.productProdVolume_df_json = new_productProdVolume_df.to_json(orient='records')


            # Construct productListFull_df
            productListFull_df = pd.merge(productSalesVolume_df, new_productProductCluster_df, on='Product', how='left')
            productListFull_df = pd.merge(productListFull_df, new_productAggLevy_df, on='Product', how='left')
            productListFull_df = pd.merge(productListFull_df, productStocks_df, on='Product', how='left')

            # Save
            quarry.productListFull_df_json = productListFull_df.to_json(orient='records')
            quarry.productAggLevy_df_json = new_productAggLevy_df.to_json(orient='records')
            quarry.productProdVolume_df_json = new_productProdVolume_df.to_json(orient='records')
            quarry.productProductCluster_df_json = new_productProductCluster_df.to_json(orient='records')
            quarry.costBoxProdVolume_df_json = new_costBoxProdVolume_df.to_json(orient='records')

            quarry.save()

            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            # Redirect the user to a specific error page for malformed JSON
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)

@login_required
def P4P_PL_allocation_view(request, project_id, quarry_id):
    # Retrieve project data or return a 404 error if not found
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve clusterToBox_df & PLAllocationData_df & productProductCluster_df & clusterToBox_df_change
    clusterToBox_df_json = quarry.clusterToBox_df_json
    clusterToBox_df = pd.read_json(StringIO(clusterToBox_df_json), orient='records')
    PLAllocationData_df_json = quarry.PLAllocationData_df_json
    PLAllocationData_df = pd.read_json(StringIO(PLAllocationData_df_json), orient='records')
    productProductCluster_df_json = quarry.productProductCluster_df_json
    productProductCluster_df = pd.read_json(StringIO(productProductCluster_df_json), orient='records')
    clusterToBox_df_change = quarry.clusterToBox_df_change

    # Generate a list of 100 numbers
    line_numbers = range(1, 101)

    # Get unique_cost_boxes
    unique_cost_boxes = clusterToBox_df['Cost_Box'].unique().tolist()

    if not PLAllocationData_df.empty and clusterToBox_df_change == False: # Existing P4P, no change in clusterToBox
        # Construct intermediate dataframe to get cost box distribution values
        cost_box_index = PLAllocationData_df.columns.get_loc("Cost_Type")
        costBox_df = PLAllocationData_df.iloc[:, cost_box_index + 1:]

        # Construct dictionary PLAllocationData_dict for saved PLAllocationData_df
        PLAllocationData_dict = []
        for index, row in PLAllocationData_df.iterrows():
            row_data = {
                'Cost_Label': row['Cost_Label'],
                'Cost': row['Cost'],
                'Select': row['Select'],
                'Cost_Type': row['Cost_Type'],
                'CostBoxValues': [costBox_df.at[index, cost_box_name] for cost_box_name in unique_cost_boxes]
            }
            PLAllocationData_dict.append(row_data)

    elif not PLAllocationData_df.empty and clusterToBox_df_change == True: # Existing P4P, change in clusterToBox
        # Construct dictionary PLAllocationData_dict for saved PLAllocationData_df
        PLAllocationData_dict = []
        for index, row in PLAllocationData_df.iterrows():
            row_data = {
                'Cost_Label': row['Cost_Label'],
                'Cost': row['Cost'],
                'Select': row['Select'],
                'Cost_Type': row['Cost_Type'],
            }
            PLAllocationData_dict.append(row_data)

    else: # New P4P
        # PLAllocationData_df is empty, initialize PLAllocationData_dict as an empty list
        PLAllocationData_dict = []

    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'currency': project_data.currency,
        'unique_cost_boxes': unique_cost_boxes,
        'project_time_period': project_data.project_time_period,
        'line_numbers': line_numbers,
        'PLAllocationData_df': PLAllocationData_df,
        'productProductCluster_df': productProductCluster_df,
        'PLAllocationData_dict': PLAllocationData_dict,
        'clusterToBox_df_change': clusterToBox_df_change,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/P4P_Portal/P4P_PL_allocation.html', context)

@login_required
def Electricity_calculator_view(request, project_id, quarry_id):
    # Retrieve project data or return a 404 error if not found
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve clusterToBox_df
    clusterToBox_df_json = quarry.clusterToBox_df_json
    clusterToBox_df = pd.read_json(StringIO(clusterToBox_df_json), orient='records')
    costBoxes = clusterToBox_df['Cost_Box'].unique().tolist()

    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'costBoxes': costBoxes,
        'project_time_period': project_data.project_time_period,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/P4P_Portal/Electricity_calculator.html', context)

@login_required
def P4P_PL_allocation_to_Results_bridge(request, project_id, quarry_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            project_data = get_object_or_404(ProjectData, project_id=project_id)
            # Retrieve the quarry based on quarry_id
            quarry = get_object_or_404(Quarry, pk=quarry_id)

            # Load json data from request body
            jsonData = json.loads(request.body)

            # Extract the data from jsonData
            PLAllocationData = jsonData

            # Retrieve existing PLAllocationData_df
            PLAllocationData_df_json = quarry.PLAllocationData_df_json
            existing_PLAllocationData_df = pd.read_json(StringIO(PLAllocationData_df_json), orient='records')

            # Convert the JSON object to a DataFrame
            new_PLAllocationData_df = pd.DataFrame(PLAllocationData[1:], columns=PLAllocationData[0])  # Skip the first row as it contains headers

            # Check if the new DataFrame is different from the existing one
            if not new_PLAllocationData_df.equals(existing_PLAllocationData_df):
                # Delete Network Optimisation Baseline and Scenarios
                # Retrieve baseline dataframes and clear them
                NO_Baseline_Done = project_data.NO_Baseline_Done
                project_data.NO_Baseline_Done = False
                project_data.save()

                # Check if scenarios exist for the project and delete them if any
                if Scenario.objects.filter(project=project_data).exists():
                    scenarios = Scenario.objects.filter(project=project_data)
                    for scenario in scenarios:
                        scenario.NO_Scenario_Raw_Results_df_json = '{}'
                        scenario.NO_Scenario_Customer_Results_df_json = '{}'
                        scenario.NO_Scenario_Done = False
                        scenario.save()

            quarry.PLAllocationData_df_json = new_PLAllocationData_df.to_json(orient='records')

            clusterToBox_df_change = False

            quarry.clusterToBox_df_change = clusterToBox_df_change
            quarry.save()


            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            print("JSONDecodeError")
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)

@login_required
def P4P_Results_view(request, project_id, quarry_id):
    # Retrieve project data or return a 404 error if not found
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    currency = project_data.currency

    # Retrieve PLAllocationData_df
    PLAllocationData_df_json = quarry.PLAllocationData_df_json
    PLAllocationData_df = pd.read_json(StringIO(PLAllocationData_df_json), orient='records')

    # Retrieve costBoxProdVolume_df
    costBoxProdVolume_df_json = quarry.costBoxProdVolume_df_json
    costBoxProdVolume_df = pd.read_json(StringIO(costBoxProdVolume_df_json), orient='records')

    # Retrieve cluster_to_box_df
    clusterToBox_df_json = quarry.clusterToBox_df_json
    clusterToBox_df = pd.read_json(StringIO(clusterToBox_df_json), orient='records')

    # Retrieve ProductProductCluster_df
    productProductCluster_df_json = quarry.productProductCluster_df_json
    productProductCluster_df = pd.read_json(StringIO(productProductCluster_df_json), orient='records')

    # Retrieve modified_transactions_df
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Retrieve productAggLevy_df
    productAggLevy_df_json = quarry.productAggLevy_df_json
    productAggLevy_df = pd.read_json(StringIO(productAggLevy_df_json), orient='records')

    # Retrieve productProdVolume_df
    productProdVolume_df_json = quarry.productProdVolume_df_json
    productProdVolume_df = pd.read_json(StringIO(productProdVolume_df_json), orient='records')


    # Cost Boxes Table
    summaryTableCostBox_dict, cluster_to_box_value_df, sorted_product_clusters, productClusterData = P4P_Cost_Box_Table(PLAllocationData_df, costBoxProdVolume_df, clusterToBox_df)

    # Cost Boxes - Clusters Table
    summaryTableCluster_df = P4P_Cost_Box_Cluster_Table(cluster_to_box_value_df, sorted_product_clusters)

    # Products Table
    summaryTableGeneral_df, P4PFullSummary_df = P4P_Products_Table(productProductCluster_df, productProdVolume_df, modified_transactions_df, quarry, productAggLevy_df, summaryTableCluster_df)

    # Charts
    plotly_Sales_Price, plotly_TM, plotly_GM, ChartAllCustomers_df = P4P_Sales_Margin_Vizualisation(modified_transactions_df, quarry, productProductCluster_df, summaryTableCluster_df,productAggLevy_df, project_data)

    # Negative Margin Table
    Margin_Table_df, Negative_Margin_Table_df, count_neg_GM, sum_neg_GM = P4P_Negative_Margin_Table(ChartAllCustomers_df)

    Download_df = P4P_Download_Data(modified_transactions_df, quarry, productProductCluster_df, summaryTableCluster_df, productAggLevy_df, currency)

    # Save productSalesProdASP_df
    quarry.P4PFullSummary_df_json = P4PFullSummary_df.to_json(orient='records')
    quarry.save()

    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'currency': project_data.currency,
        'project_time_period': project_data.project_time_period,
        'summaryTableCostBox_dict': summaryTableCostBox_dict,
        'summaryTableCluster_df': summaryTableCluster_df,
        'productClusterData': productClusterData,
        'summaryTableGeneral_df': summaryTableGeneral_df,
        'plotly_Sales_Price': plotly_Sales_Price,
        'plotly_TM': plotly_TM,
        'plotly_GM': plotly_GM,
        'Negative_Margin_Table_df': Negative_Margin_Table_df,
        'count_neg_GM': count_neg_GM,
        'sum_neg_GM': sum_neg_GM,
        'Download_df': Download_df.to_json(orient='split'),  # Convert DataFrame to JSON
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/P4P_Portal/P4P_Results.html', context)




# Existing Projects / Data Portal / B4C (8 views)
@login_required
def B4C_Portal_view(request, project_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

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

    is_editor = request.user.groups.filter(name='Editors').exists()

    context = {
        'project_id': project_id,
        'project_data': project_data,
        'quarries_list': quarries_list,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'B4C_empty_df': B4C_empty_df,
        'is_editor': is_editor,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/B4C_Portal/B4C_Portal.html', context)

@login_required
def B4C_Product_Composition_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve modified_transaction_df
    modified_transactions_df_json = project_data.modified_transactions_df_json
    modified_transactions_df = pd.read_json(StringIO(modified_transactions_df_json), orient='records')

    # Retrieve productProdVolume_df_json
    productProdVolume_df_json = quarry.productProdVolume_df_json
    productProdVolume_df = pd.read_json(StringIO(productProdVolume_df_json), orient='records')

    # Get the name of the quarry
    quarry_name = quarry.name

    # Select the needed columns from modified_transactions_df
    selected_columns_df = modified_transactions_df[["Quarries", "Product", "Sales_Volume"]]

    # Filter the "Quarries" column based on the name of the quarry
    selected_columns_filtered_df = selected_columns_df[selected_columns_df["Quarries"] == quarry_name]

    # Group by "Product" and aggregate the sum of volumes
    product_list_df = selected_columns_filtered_df.groupby("Product").agg(
        Sales_Volume=("Sales_Volume", lambda x: round(x.sum()))  # Sum the volumes and round to the nearest integer
    ).reset_index()

    # Sort the DataFrame by the "Volume" column in descending order
    product_list_df = product_list_df.sort_values(by="Sales_Volume", ascending=False)
    product_list_df = pd.merge(product_list_df, productProdVolume_df, on='Product', how='left')
    product_list_df = product_list_df.rename(columns={'Production Volume': 'Production_Volume'})
    product_list_df['Sales_Volume'] = product_list_df['Sales_Volume'].apply(format_with_commas)
    product_list_df['Production_Volume'] = product_list_df['Production_Volume'].apply(format_with_commas)

    # Retrieve productComposition_df
    productComposition_df_json = quarry.productComposition_df_json
    productComposition_df = pd.read_json(StringIO(productComposition_df_json), orient='records')

    # Processing data to match constituents with products
    processed_data = []

    # Initialize unique_constituents to an empty list
    unique_constituents = []

    # Check if productComposition_df is empty
    if not productComposition_df.empty:
        # Get unique constituents as headers
        unique_constituents = productComposition_df['Constituent'].unique()

        for _, row in product_list_df.iterrows():
            product_data = {
                'Product': row['Product'],
                'Sales_Volume': row['Sales_Volume'],
                'Production_Volume': row['Production_Volume'],
                'Constituents': []  # List to hold constituent data for the current product
            }
            for constituent in unique_constituents:
                # Filter productComposition_df based on both Product and Constituent
                filtered_df = productComposition_df[(productComposition_df['Product'] == row['Product']) &
                                                    (productComposition_df['Constituent'] == constituent)]
                if not filtered_df.empty:
                    # If there's a match, append the composition value
                    product_data['Constituents'].append(filtered_df['Composition'].iloc[0])
                else:
                    product_data['Constituents'].append(None)  # or any placeholder value
            processed_data.append(product_data)
    else:
        # If productComposition_df is empty, set processed_data to an empty list
        processed_data = []

    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'product_list_df': product_list_df,
        'project_time_period': project_data.project_time_period,
        'productComposition_df': productComposition_df,
        'unique_constituents': unique_constituents,
        'processed_data': processed_data,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/B4C_Portal/B4C_Product_Composition.html', context)

@login_required
def B4C_Product_Composition_to_Production_Balance_bridge(request, project_id, quarry_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            project_data = get_object_or_404(ProjectData, project_id=project_id)
            # Retrieve the quarry based on quarry_id
            quarry = get_object_or_404(Quarry, pk=quarry_id)

            # Load json data from request body
            jsonData = json.loads(request.body)

            # Extract the concatenated data
            concatenatedData = jsonData['extractedData']

            # Extract the concatenated data
            header = concatenatedData['header']
            data = concatenatedData['data']

            # Construct the DataFrame
            productComposition_df = pd.DataFrame(data, columns=header)

            # Drop Sales Volume column
            productComposition_df = productComposition_df.drop(columns=['Sales Volume [t]'])

            # Rename column and remove "," thousands  delimiter
            productComposition_df = productComposition_df.rename(columns={'Production Volume [t]': 'Production_Volume'})
            productComposition_df['Production_Volume'] = productComposition_df['Production_Volume'].str.replace(',', '')

            # Get the columns to melt (all columns after 'Production_Volume')
            columns_to_melt = productComposition_df.columns[2:]

            # Melt the DataFrame
            new_productComposition_df = pd.melt(productComposition_df, id_vars=['Product', 'Production_Volume'],value_vars=columns_to_melt, var_name='Constituent', value_name='Composition')

            # Replace '%' and empty strings by '0' in the "Composition" column, then convert to integers
            new_productComposition_df['Composition'] = new_productComposition_df['Composition'].replace({'%': '0', '': '0'},regex=True).astype(int)
            new_productComposition_df['Production_Volume'] = new_productComposition_df['Production_Volume'].astype(int)
            new_productComposition_df['Composition'] = new_productComposition_df['Composition'].astype(int)

            # Retrieve existing productComposition_df
            productComposition_df_json = quarry.productComposition_df_json
            existing_productComposition_df = pd.read_json(StringIO(productComposition_df_json), orient='records')
            if not existing_productComposition_df.empty:
                existing_productComposition_df['Production_Volume'] = existing_productComposition_df['Production_Volume'].astype(int)
                existing_productComposition_df['Composition'] = existing_productComposition_df['Composition'].astype(int)


            # Check if the new DataFrame is different from the existing one
            if not new_productComposition_df.equals(existing_productComposition_df):
                # Delete Network Optimisation Baseline and Scenarios
                # Retrieve baseline dataframes and clear them
                NO_Baseline_Done = project_data.NO_Baseline_Done
                project_data.NO_Baseline_Done = False
                project_data.save()

                # Check if scenarios exist for the project and delete them if any
                if Scenario.objects.filter(project=project_data).exists():
                    scenarios = Scenario.objects.filter(project=project_data)
                    for scenario in scenarios:
                        scenario.NO_Scenario_Raw_Results_df_json = '{}'
                        scenario.NO_Scenario_Customer_Results_df_json = '{}'
                        scenario.NO_Scenario_Done = False
                        scenario.save()

                # Empty constituentModesBalance_df & capacityModes_df (Production Balance view)
                # Retrieve constituentModesBalance_df & capacityModes_df
                constituentModesBalance_df_json = quarry.constituentModesBalance_df_json
                constituentModesBalance_df = pd.read_json(StringIO(constituentModesBalance_df_json), orient='records')
                constituentModesBalance_df = pd.DataFrame()
                capacityModes_df_json = quarry.capacityModes_df_json
                capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
                capacityModes_df = pd.DataFrame()

                # Save constituentModesBalance_df & capacityModes_df
                quarry.constituentModesBalance_df_json = constituentModesBalance_df.to_json(orient='records')
                quarry.capacityModes_df_json = capacityModes_df.to_json(orient='records')
                quarry.save()

            # Save productComposition_df
            quarry.productComposition_df_json = new_productComposition_df.to_json(orient='records')
            quarry.save()

            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            print("JSONDecodeError")
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)

@login_required
def B4C_Production_Balance_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve productComposition_df
    productComposition_df_json = quarry.productComposition_df_json
    productComposition_df = pd.read_json(StringIO(productComposition_df_json), orient='records')

    # Calculate Constituent Production Volume
    constituentProduction_df = productComposition_df.copy()
    constituentProduction_df['Constituent_Volume'] = constituentProduction_df['Production_Volume'] * constituentProduction_df['Composition'] / 100
    constituentProduction_df = constituentProduction_df.drop(columns=['Product', 'Production_Volume', 'Composition'])
    constituentProduction_df = constituentProduction_df.groupby('Constituent')['Constituent_Volume'].sum().reset_index()
    constituentProduction_df['Constituent_Volume'] = constituentProduction_df['Constituent_Volume'].astype(int)
    constituentProduction_df = constituentProduction_df.sort_values(by='Constituent_Volume', ascending=False)
    referenceBalance_df = constituentProduction_df.copy()
    constituentProduction_df['Constituent_Volume'] = constituentProduction_df['Constituent_Volume'].apply(format_with_commas)


    # Retrieve constituentModesBalance_df & capacityModes_df
    constituentModesBalance_df_json = quarry.constituentModesBalance_df_json
    constituentModesBalance_df = pd.read_json(StringIO(constituentModesBalance_df_json), orient='records')
    capacityModes_df_json = quarry.capacityModes_df_json
    capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')


    # Processing data to match constituents with products
    processed_data = []

    # Initialize unique_modes to an empty list
    unique_modes = []

    # Check if constituentModesBalance_df is empty
    if not constituentModesBalance_df.empty:
        # Merge dataframes to get Production_Volume
        constituentModesBalance_df = pd.merge(constituentModesBalance_df, constituentProduction_df, on="Constituent",
                                              how='left')
        # Get unique modes as headers
        unique_modes = capacityModes_df['Modes'].unique()

        # Initialize a set to store processed constituents
        processed_constituents = set()

        for _, row in constituentModesBalance_df.iterrows():
            # Check if the constituent has already been processed
            if row['Constituent'] not in processed_constituents:
                processed_constituents.add(row['Constituent'])

                product_data = {
                    'Constituent': row['Constituent'],
                    'Constituent_Volume': row['Constituent_Volume'],
                    'Modes': []  # List to hold modes data for the current product
                }
                for mode in unique_modes:
                    # Filter constituentModesBalance_df based on both Constituent and Modes
                    filtered_df = constituentModesBalance_df[
                        (constituentModesBalance_df['Constituent'] == row['Constituent']) &
                        (constituentModesBalance_df['Modes'] == mode)]
                    if not filtered_df.empty:
                        # If there's a match, append the Balance value
                        product_data['Modes'].append(filtered_df['Balance'].iloc[0])
                    else:
                        product_data['Modes'].append(None)  # or any placeholder value
                processed_data.append(product_data)
    else:
        # If constituentModesBalance_df is empty, set processed_data to an empty list
        processed_data = []

    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'project_time_period': project_data.project_time_period,
        'constituentProduction_df': constituentProduction_df,
        'referenceBalance_df': referenceBalance_df,
        'constituentModesBalance_df': constituentModesBalance_df,
        'unique_modes': unique_modes,
        'processed_data': processed_data,
        'capacityModes_df': capacityModes_df,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/B4C_Portal/B4C_Production_Balance.html', context)

@login_required
def B4C_Production_Balance_to_Results_bridge(request, project_id, quarry_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            project_data = get_object_or_404(ProjectData, project_id=project_id)
            # Retrieve the quarry based on quarry_id
            quarry = get_object_or_404(Quarry, pk=quarry_id)

            # Load json data from request body & extract the data & convert to dataframe
            jsonData = json.loads(request.body)
            productionModes = jsonData
            productionModes_df = pd.DataFrame(productionModes)

            # Set the first row as column names
            productionModes_df.columns = productionModes_df.iloc[0]
            productionModes_df = productionModes_df.drop(0)

            # Dataframe cleaning
            productionModes_df = productionModes_df.drop(columns=['Production Volume [t]'])
            productionModes_df = productionModes_df[productionModes_df.iloc[:, 0] != 'Validation']
            productionModes_df = productionModes_df.replace({'%': '0', '': '0'}, regex=True)
            for column in productionModes_df.columns[1:]:  # Exclude the first column
                productionModes_df[column] = pd.to_numeric(productionModes_df[column], errors='coerce')

            # Split the DataFrame into two DataFrames
            constituentModesBalance_df = productionModes_df.iloc[:-2]  # Select all rows except the last two
            capacityModes_df = productionModes_df.iloc[-2:]  # Select the last two rows

            # Set up constituentModesBalance_df
            constituentModesBalance_df = pd.melt(constituentModesBalance_df, id_vars=['Constituent'], var_name='Modes', value_name='Balance')
            new_constituentModesBalance_df = constituentModesBalance_df.rename(columns={'Constituent': 'Constituent'})
            new_constituentModesBalance_df['Balance'] = new_constituentModesBalance_df['Balance'].astype(int)

            # Set up capacityModes_df
            capacityModes_df = capacityModes_df.rename(columns={capacityModes_df.columns[0]: 'Parameter'})
            # Transpose the dataframe
            capacityModes_df = capacityModes_df.T.reset_index()
            # Set the first row as column names
            capacityModes_df.columns = capacityModes_df.iloc[0]
            # Drop the first row since it's now the column names
            capacityModes_df = capacityModes_df.drop(0)
            new_capacityModes_df = capacityModes_df.rename(columns={'Parameter': 'Modes', 'Max Capacity [t/h]': 'Max_capacity', 'Coefficient [0]': 'Coefficient'})
            # Convert new_capacityModes_df to json and back to set the first row as column name and allow the comparison
            capacityModes_df_json = new_capacityModes_df.to_json(orient='records')
            new_capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')

            # Retrieve existing capacityModes_df and existing constituentModesBalance_df
            capacityModes_df_json = quarry.capacityModes_df_json
            existing_capacityModes_df = pd.read_json(StringIO(capacityModes_df_json), orient='records')
            constituentModesBalance_df_json = quarry.constituentModesBalance_df_json
            existing_constituentModesBalance_df = pd.read_json(StringIO(constituentModesBalance_df_json), orient='records')
            if not existing_constituentModesBalance_df.empty:
                existing_constituentModesBalance_df['Balance'] = existing_constituentModesBalance_df['Balance'].astype(int)


            # Check if the new DataFrame is different from the existing one
            if not new_constituentModesBalance_df.equals(existing_constituentModesBalance_df) or not new_capacityModes_df.equals(existing_capacityModes_df):
                # Delete Network Optimisation Baseline and Scenarios
                # Retrieve baseline dataframes and clear them
                NO_Baseline_Done = project_data.NO_Baseline_Done
                project_data.NO_Baseline_Done = False
                project_data.save()

                # Check if scenarios exist for the project and delete them if any
                if Scenario.objects.filter(project=project_data).exists():
                    scenarios = Scenario.objects.filter(project=project_data)
                    for scenario in scenarios:
                        scenario.NO_Scenario_Raw_Results_df_json = '{}'
                        scenario.NO_Scenario_Customer_Results_df_json = '{}'
                        scenario.NO_Scenario_Done = False
                        scenario.save()


            # Save constituentModesBalance_df & capacityModes_df
            quarry.constituentModesBalance_df_json = new_constituentModesBalance_df.to_json(orient='records')
            quarry.capacityModes_df_json = new_capacityModes_df.to_json(orient='records')
            quarry.save()

            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            print("JSONDecodeError")
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)

@login_required
def B4C_Results_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # === Min Variable Cost ===
    # Define model
    mod_file = 'B4C_minVC_Base'
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
        dat_file_path = os.path.join(temp_dir, 'B4C_minVC_Base.dat')
        result_file_path = os.path.join(temp_dir, 'B4C_minVC_Base.sol')

        # Ensure that the .dat file is created correctly in the temporary directory
        B4C_Results.B4C_dat_minVC_Base(quarry, dat_file_path)

        # Perform the optimization
        B4C_dat_minVC_results_df = B4C_Results.B4C_GLPK_minVC(mod_file_path, dat_file_path, result_file_path)

        # Get MinVC_Base_Table_df
        minVC_Base_Table_df, sum_row_minVC_Base_Table, sum_row_minVC_Base_Table_Delta = B4C_Results.B4C_Create_minVC_Base_Table(quarry, B4C_dat_minVC_results_df)
        # Get minVC_modes_Hours_df
        minVC_modes_Hours_df, minVC_sum_row_modes_Hours_df = B4C_Results.B4C_Create_modes_Hours(B4C_dat_minVC_results_df, quarry)
        # Get minVC_Base_Table_df_Downaload
        minVC_Base_Table_df_Downaload = B4C_Results.B4C_Create_Download_minVC(project_data, quarry)


    # === Max Gross Margin ===
    # Define model
    mod_file = 'B4C_maxGM_Base'
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
        dat_file_path = os.path.join(temp_dir, 'B4C_maxGM_Base.dat')
        result_file_path = os.path.join(temp_dir, 'B4C_maxGM_Base.sol')

        # Ensure that the .dat file is created correctly in the temporary directory
        customersSalesASPProduct_df = B4C_Results.B4C_dat_maxGM_Base(quarry, project_data, dat_file_path)

        # Perform the optimization
        B4C_dat_maxGM_results_df = B4C_Results.B4C_GLPK_maxGM(mod_file_path, dat_file_path, result_file_path)

        # Get MaxGM_Base_Table_df
        maxGM_Base_Table_df, sum_row_maxGM_Base_Table, maxGM_Base_Table_df_Downaload, sum_row_maxGM_Base_Table_Delta = B4C_Results.B4C_Create_maxGM_Base_Table(quarry, B4C_dat_maxGM_results_df, customersSalesASPProduct_df)
        # Get maxGM_modes_Hours_df
        maxGM_modes_Hours_df, maxGM_sum_row_modes_Hours_df = B4C_Results.B4C_Create_modes_Hours(B4C_dat_maxGM_results_df, quarry)

        # === Delta ===
        minVC_maxGM_difference_df, minVC_maxGM_difference_df_Download = B4C_Results.B4C_Create_minVC_maxGM_difference_df(minVC_Base_Table_df_Downaload, maxGM_Base_Table_df_Downaload)
        delta_row_minVC_maxGM, sum_operating_hours, sum_opening_hours = B4C_Results.B4C_Summary_Delta(sum_row_minVC_Base_Table_Delta, sum_row_maxGM_Base_Table_Delta, minVC_sum_row_modes_Hours_df, maxGM_sum_row_modes_Hours_df)

    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
        'minVC_Base_Table_df': minVC_Base_Table_df,
        'sum_row_minVC_Base_Table': sum_row_minVC_Base_Table,
        'maxGM_Base_Table_df': maxGM_Base_Table_df,
        'sum_row_maxGM_Base_Table': sum_row_maxGM_Base_Table,
        'minVC_modes_Hours_df': minVC_modes_Hours_df,
        'minVC_sum_row_modes_Hours_df': minVC_sum_row_modes_Hours_df,
        'minVC_Base_Table_df_Downaload': minVC_Base_Table_df_Downaload.to_json(orient='split'),  # Convert DataFrame to JSON
        'maxGM_modes_Hours_df': maxGM_modes_Hours_df,
        'maxGM_sum_row_modes_Hours_df': maxGM_sum_row_modes_Hours_df,
        'maxGM_Base_Table_df_Downaload': maxGM_Base_Table_df_Downaload.to_json(orient='split'),  # Convert DataFrame to JSON
        'minVC_maxGM_difference_df': minVC_maxGM_difference_df,
        'minVC_maxGM_difference_df_Download': minVC_maxGM_difference_df_Download.to_json(orient='split'),  # Convert DataFrame to JSON
        'delta_row_minVC_maxGM': delta_row_minVC_maxGM,
        'sum_operating_hours': sum_operating_hours,
        'sum_opening_hours': sum_opening_hours,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/B4C_Portal/B4C_Results.html', context)

@login_required
def B4C_Simulator_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Get dataframes to be displayed in HTML template
    B4CSimulation_df = B4C_Simulator.B4C_Get_prodSalesASP(quarry)
    minVC_Simulation_Table_df, modes_Hours_minVC_Simulation_df, maxGM_Simulation_Table_df, modes_Hours_maxGM_Simulation_df = B4C_Simulator.Retrieve_dataframes(quarry)

    # Check if minVC_Simulation_Table_df is empty
    if minVC_Simulation_Table_df.empty:
        context = {
            'project_data': project_data,
            'quarry': quarry,
            'project_id': project_id,
            'quarry_id': quarry_id,
            'project_name': project_data.project_name,
            'quarry_name': quarry.name,
            'project_time_period': project_data.project_time_period,
            'currency': project_data.currency,
            'B4CSimulation_df': B4CSimulation_df,
        }
        return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/B4C_Portal/B4C_Simulator.html', context)

    else:
        # Calculate the sums
        sum_row_minVC_Simulation_Table = minVC_Simulation_Table_df.sum(axis=0)
        sum_row_minVC_Simulation_Table = sum_row_minVC_Simulation_Table.drop(['Product'])
        sum_row_maxGM_Simulation_Table = maxGM_Simulation_Table_df.sum(axis=0)
        sum_row_maxGM_Simulation_Table = sum_row_maxGM_Simulation_Table.drop(['Product'])

        sum_row_minVC_modes_Hours = modes_Hours_minVC_Simulation_df.sum(axis=0)
        # Round 'Operating_Hours' and 'Opening_Hours' to 1 decimal place
        sum_row_minVC_modes_Hours['Operating_Hours'] = round(sum_row_minVC_modes_Hours['Operating_Hours'], 1)
        sum_row_minVC_modes_Hours['Opening_Hours'] = round(sum_row_minVC_modes_Hours['Opening_Hours'], 1)

        sum_row_maxGM_modes_Hours = modes_Hours_maxGM_Simulation_df.sum(axis=0)
        # Round 'Operating_Hours' and 'Opening_Hours' to 1 decimal place
        sum_row_maxGM_modes_Hours['Operating_Hours'] = round(sum_row_maxGM_modes_Hours['Operating_Hours'], 1)
        sum_row_maxGM_modes_Hours['Opening_Hours'] = round(sum_row_maxGM_modes_Hours['Opening_Hours'], 1)

        # Calculate Delta
        delta_row_minVC_maxGM, sum_operating_hours, sum_opening_hours = B4C_Simulator.B4C_Summary_Delta(sum_row_minVC_Simulation_Table, sum_row_maxGM_Simulation_Table, sum_row_minVC_modes_Hours, sum_row_maxGM_modes_Hours)

        # Format data
        minVC_Simulation_Table_df['Sales_Volume_Sim'] = minVC_Simulation_Table_df['Sales_Volume_Sim'].apply(format_with_commas)
        maxGM_Simulation_Table_df['Sales_Volume_Sim'] = maxGM_Simulation_Table_df['Sales_Volume_Sim'].apply(format_with_commas)
        minVC_Simulation_Table_df['Min_Closing_Stock'] = minVC_Simulation_Table_df['Min_Closing_Stock'].apply(format_with_commas)
        maxGM_Simulation_Table_df['Min_Closing_Stock'] = maxGM_Simulation_Table_df['Min_Closing_Stock'].apply(format_with_commas)
        minVC_Simulation_Table_df['Turnover'] = minVC_Simulation_Table_df['Turnover'].apply(format_with_commas)
        maxGM_Simulation_Table_df['Turnover'] = maxGM_Simulation_Table_df['Turnover'].apply(format_with_commas)
        minVC_Simulation_Table_df['Variable_Cost'] = minVC_Simulation_Table_df['Variable_Cost'].apply(format_with_commas)
        maxGM_Simulation_Table_df['Variable_Cost'] = maxGM_Simulation_Table_df['Variable_Cost'].apply(format_with_commas)
        minVC_Simulation_Table_df['Gross_Margin'] = minVC_Simulation_Table_df['Gross_Margin'].apply(format_with_commas)
        maxGM_Simulation_Table_df['Gross_Margin'] = maxGM_Simulation_Table_df['Gross_Margin'].apply(format_with_commas)
        minVC_Simulation_Table_df['ASP_Sim'] = minVC_Simulation_Table_df['ASP_Sim'].round(2)
        maxGM_Simulation_Table_df['ASP_Sim'] = maxGM_Simulation_Table_df['ASP_Sim'].round(2)
        minVC_Simulation_Table_df['Variable_Cost_per_t'] = minVC_Simulation_Table_df['Variable_Cost_per_t'].round(2)
        maxGM_Simulation_Table_df['Variable_Cost_per_t'] = maxGM_Simulation_Table_df['Variable_Cost_per_t'].round(2)
        sum_row_minVC_Simulation_Table = sum_row_minVC_Simulation_Table.apply(format_with_commas)
        sum_row_maxGM_Simulation_Table = sum_row_maxGM_Simulation_Table.apply(format_with_commas)
        modes_Hours_minVC_Simulation_df['Coefficient'] = modes_Hours_minVC_Simulation_df['Coefficient'].round(1)
        modes_Hours_minVC_Simulation_df['Operating_Hours'] = modes_Hours_minVC_Simulation_df['Operating_Hours'].round(1)
        modes_Hours_minVC_Simulation_df['Opening_Hours'] = modes_Hours_minVC_Simulation_df['Opening_Hours'].round(1)
        modes_Hours_maxGM_Simulation_df['Coefficient'] = modes_Hours_maxGM_Simulation_df['Coefficient'].round(1)
        modes_Hours_maxGM_Simulation_df['Operating_Hours'] = modes_Hours_maxGM_Simulation_df['Operating_Hours'].round(1)
        modes_Hours_maxGM_Simulation_df['Opening_Hours'] = modes_Hours_maxGM_Simulation_df['Opening_Hours'].round(1)


        context = {
            'project_data': project_data,
            'quarry': quarry,
            'project_id': project_id,
            'quarry_id': quarry_id,
            'project_name': project_data.project_name,
            'quarry_name': quarry.name,
            'project_time_period': project_data.project_time_period,
            'currency': project_data.currency,

            'B4CSimulation_df': B4CSimulation_df,

            'minVC_Simulation_Table_df': minVC_Simulation_Table_df,
            'sum_row_minVC_Simulation_Table': sum_row_minVC_Simulation_Table,

            'modes_Hours_minVC_Simulation_df': modes_Hours_minVC_Simulation_df,
            'sum_row_minVC_modes_Hours': sum_row_minVC_modes_Hours,

            'maxGM_Simulation_Table_df': maxGM_Simulation_Table_df,
            'sum_row_maxGM_Simulation_Table': sum_row_maxGM_Simulation_Table,

            'modes_Hours_maxGM_Simulation_df': modes_Hours_maxGM_Simulation_df,
            'sum_row_maxGM_modes_Hours': sum_row_maxGM_modes_Hours,

            'delta_row_minVC_maxGM': delta_row_minVC_maxGM,
            'sum_operating_hours': sum_operating_hours,
            'sum_opening_hours': sum_opening_hours,
        }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/B4C_Portal/B4C_Simulator.html', context)

@login_required
def B4C_Simulator_bridge(request, project_id, quarry_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            get_object_or_404(ProjectData, project_id=project_id)
            # Retrieve the quarry based on quarry_id
            quarry = get_object_or_404(Quarry, pk=quarry_id)

            # Load json data from request body & extract the data & convert to dataframe
            jsonData = json.loads(request.body)
            salesSimulation = jsonData
            salesSimulation_df = pd.DataFrame(salesSimulation)

            # Set the first row as column names
            salesSimulation_df.columns = salesSimulation_df.iloc[0]
            salesSimulation_df = salesSimulation_df.drop(0)

            # Replace empty strings with 0 and convert to numeric values
            salesSimulation_df['Sales_Volume_Sim'] = salesSimulation_df['Sales_Volume_Sim'].replace({',': '', '': '0'}, regex=True)
            salesSimulation_df['Sales_Volume_Sim'] = salesSimulation_df['Sales_Volume_Sim'].astype(int)
            salesSimulation_df['ASP_Sim'] = salesSimulation_df['ASP_Sim'].replace('', '0').astype(float).round(2)
            salesSimulation_df['Opening_Stock_Sim'] = salesSimulation_df['Opening_Stock_Sim'].replace({',': '', '': '0'}, regex=True)
            salesSimulation_df['Opening_Stock_Sim'] = salesSimulation_df['Opening_Stock_Sim'].astype(int)
            salesSimulation_df['Closing_Stock_Sim'] = salesSimulation_df['Closing_Stock_Sim'].replace({',': '', '': '0'}, regex=True)
            salesSimulation_df['Closing_Stock_Sim'] = salesSimulation_df['Closing_Stock_Sim'].astype(int)


            # === Min VC Simulation ===
            # Define model
            mod_file = 'B4C_minVC_Simulation'
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
                dat_file_path = os.path.join(temp_dir, 'B4C_minVC_Simulation.dat')
                result_file_path = os.path.join(temp_dir, 'B4C_minVC_Simulation.sol')

                # Create .dat for minVC Simulations
                B4C_Simulator.B4C_dat_minVC_Simulation(quarry, salesSimulation_df, dat_file_path)

                # Run optimisation
                B4C_dat_minVC_Simulation_results_df = B4C_Simulator.B4C_GLPK_minVC_Simulation(mod_file_path, dat_file_path, result_file_path)

                # Get minVC_Simulation_Table_df
                minVC_Simulation_Table_df = B4C_Simulator.B4C_Create_minVC_Simulation_Table(quarry, B4C_dat_minVC_Simulation_results_df, salesSimulation_df)
                # Get modes_Hours_minVC_Simulation_df
                modes_Hours_minVC_Simulation_df = B4C_Simulator.B4C_Create_modes_Hours_minVC(B4C_dat_minVC_Simulation_results_df, quarry)

            # === Max GM Simulation ===
            # Define model
            mod_file = 'B4C_maxGM_Simulation'
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
                dat_file_path = os.path.join(temp_dir, 'B4C_maxGM_Simulation.dat')
                result_file_path = os.path.join(temp_dir, 'B4C_maxGM_Simulation.sol')

                # Create .dat for maxGM Simulations
                B4C_Simulator.B4C_dat_maxGM_Simulation(quarry, salesSimulation_df, dat_file_path)

                # Run optimisation
                B4C_dat_maxGM_Simulation_results_df = B4C_Simulator.B4C_GLPK_maxGM_Simulation(mod_file_path, dat_file_path, result_file_path)

                # Get maxGM_Simulation_Table_df
                maxGM_Simulation_Table_df = B4C_Simulator.B4C_Create_maxGM_Simulation_Table(quarry, B4C_dat_maxGM_Simulation_results_df, salesSimulation_df)
                # Get modes_Hours_maxGM_Simulation_df
                modes_Hours_maxGM_Simulation_df = B4C_Simulator.B4C_Create_modes_Hours_maxGM(B4C_dat_maxGM_Simulation_results_df, quarry)

            # Store dataframes
            quarry.salesSimulation_df_json = salesSimulation_df.to_json(orient='records')
            quarry.minVC_Simulation_Table_df_json = minVC_Simulation_Table_df.to_json(orient='records')
            quarry.modes_Hours_minVC_Simulation_df_json = modes_Hours_minVC_Simulation_df.to_json(orient='records')
            quarry.maxGM_Simulation_Table_df_json = maxGM_Simulation_Table_df.to_json(orient='records')
            quarry.modes_Hours_maxGM_Simulation_df_json = modes_Hours_maxGM_Simulation_df.to_json(orient='records')
            quarry.save()

            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            print("JSONDecodeError")
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)




# Existing Projects / Data Portal / FarSeer (9 views)
@login_required
def FarSeer_Portal_view(request, project_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
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

    is_editor = request.user.groups.filter(name='Editors').exists()

    context = {
        'project_id': project_id,
        'project_data': project_data,
        'quarries_list': quarries_list,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'FarSeer_empty_df': FarSeer_empty_df,
        'is_editor': is_editor,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/FarSeer_Portal/FarSeer_Portal.html', context)

@login_required
def FarSeer_Information_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)
    # Retrieve dataframes
    P4PFullSummary_df_json = quarry.P4PFullSummary_df_json
    P4PFullSummary_df = pd.read_json(StringIO(P4PFullSummary_df_json), orient='records')
    FarSeer_stockInfo_df_json = quarry.FarSeer_stockInfo_df_json
    FarSeer_stockInfo_df = pd.read_json(StringIO(FarSeer_stockInfo_df_json), orient='records')

    # Format stockInfo_df
    FarSeer_stockInfo_df.replace(0, '', inplace=True)

    unique_products = P4PFullSummary_df['Product'].unique()

    # Define a list of month names using calendar module
    months = [calendar.month_name[i] for i in range(1, 13)]

    # Get startingMonth and finalMonth from quarry
    starting_month = quarry.startingMonth
    final_month = quarry.finalMonth


    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
        'unique_products': unique_products,
        'months': months,
        'starting_month': starting_month,
        'final_month': final_month,
        'FarSeer_stockInfo_df': FarSeer_stockInfo_df,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/FarSeer_Portal/FarSeer_Information.html', context)

@login_required
def FarSeer_Information_to_Forecast_bridge(request, project_id, quarry_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            get_object_or_404(ProjectData, project_id=project_id)
            # Retrieve the quarry based on quarry_id
            quarry = get_object_or_404(Quarry, pk=quarry_id)

            # Load json data from request body & extract the data & convert to dataframe
            jsonData = json.loads(request.body)

            # Extract the three variables from jsonData
            stockInfo = jsonData['tableData']
            startingMonth = jsonData['startingMonth']
            finalMonth = jsonData['finalMonth']

            # Convert to dataframe
            FarSeer_stockInfo_df = pd.DataFrame(stockInfo)
            # Set the first row as column names and drop the first row
            FarSeer_stockInfo_df.columns = FarSeer_stockInfo_df.iloc[0]
            FarSeer_stockInfo_df = FarSeer_stockInfo_df.drop(0)

            # Rename columns and format data
            FarSeer_stockInfo_df = FarSeer_stockInfo_df.rename(
                columns={'Opening Stock at Starting Month [t]': 'Stock_Starting_Month',
                         'Min. Closing Stock at Final Month [t]': 'Stock_Final_Month'})

            FarSeer_stockInfo_df['Stock_Starting_Month'] = FarSeer_stockInfo_df['Stock_Starting_Month'].replace({'': '0'}, regex=True)
            FarSeer_stockInfo_df['Stock_Starting_Month'] = FarSeer_stockInfo_df['Stock_Starting_Month'].astype(int)
            FarSeer_stockInfo_df['Stock_Final_Month'] = FarSeer_stockInfo_df['Stock_Final_Month'].replace({'': '0'}, regex=True)
            FarSeer_stockInfo_df['Stock_Final_Month'] = FarSeer_stockInfo_df['Stock_Final_Month'].astype(int)

            quarry.FarSeer_stockInfo_df_json = FarSeer_stockInfo_df.to_json(orient='records')
            quarry.startingMonth = startingMonth
            quarry.finalMonth = finalMonth
            quarry.save()

            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            print("JSONDecodeError")
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)

@login_required
def FarSeer_Forecast_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve dataframes
    maxOpeningHours_df_json = quarry.maxOpeningHours_df_json
    maxOpeningHours_df = pd.read_json(StringIO(maxOpeningHours_df_json), orient='records')
    salesForecast_df_json = quarry.salesForecast_df_json
    salesForecast_df = pd.read_json(StringIO(salesForecast_df_json), orient='records')

    SalesPerMonth_df = FarSeer.FarSeer_Create_SalesPerMonth_df(project_data, quarry)
    maxOpeningHours_Table = FarSeer.FarSeer_maxOpeningHours_Table(SalesPerMonth_df, maxOpeningHours_df, quarry)
    salesForecast_Table = FarSeer.FarSeer_salesForecast_Table(SalesPerMonth_df, salesForecast_df)

    unique_months = SalesPerMonth_df['Month_Name'].unique()

    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
        'unique_months': unique_months,
        'maxOpeningHours_Table': maxOpeningHours_Table,
        'salesForecast_Table': salesForecast_Table,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/FarSeer_Portal/FarSeer_Forecast.html', context)

@login_required
def FarSeer_Forecast_to_Results_bridge(request, project_id, quarry_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            get_object_or_404(ProjectData, project_id=project_id)
            # Retrieve the quarry based on quarry_id
            quarry = get_object_or_404(Quarry, pk=quarry_id)

            # Load json data from request body & extract the data & convert to dataframe
            jsonData = json.loads(request.body)

            # Extract the two variables from jsonData
            forecast = jsonData['forecast']
            maxOpeningHours = jsonData['maxHours']

            # Convert the JSON object to a DataFrame
            salesForecast_df = pd.DataFrame(forecast)
            maxOpeningHours_df = pd.DataFrame(maxOpeningHours)
            # Set the first row as column names and drop the first row
            salesForecast_df.columns = salesForecast_df.iloc[0]
            salesForecast_df = salesForecast_df.drop(0)
            maxOpeningHours_df.columns = maxOpeningHours_df.iloc[0]
            maxOpeningHours_df = maxOpeningHours_df.drop(0)

            # Reformat data
            salesForecast_df, maxOpeningHours_df = FarSeer.FarSeer_Format_salesForecast_df(salesForecast_df, maxOpeningHours_df)

            # Save
            quarry.maxOpeningHours_df_json = maxOpeningHours_df.to_json(orient='records')
            quarry.salesForecast_df_json = salesForecast_df.to_json(orient='records')
            quarry.save()

            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            print("JSONDecodeError")
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)

@login_required
def FarSeer_Clear_Data(*args, **kwargs):
    project_id = kwargs.get('project_id')
    quarry_id = kwargs.get('quarry_id')

    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Delete Data
    quarry.startingMonth = ''
    quarry.finalMonth = ''
    quarry.FarSeer_stockInfo_df_json = '{}'
    quarry.maxOpeningHours_df_json = '{}'
    quarry.salesForecast_df_json = '{}'
    quarry.FarSeer_meetAllSales_results_df_json = '{}'
    quarry.FarSeer_meetAllSales_hours_df_json = '{}'
    quarry.FarSeer_meetForcedSales_results_df_json = '{}'
    quarry.FarSeer_meetForcedSales_hours_df_json = '{}'
    quarry.FarSeer_maxGM_results_df_json = '{}'
    quarry.FarSeer_maxGM_hours_df_json = '{}'

    # Save the changes
    quarry.save()

    url = reverse('FarSeer_Portal', kwargs={'project_id': project_id})
    return HttpResponseRedirect(url)


@login_required
def FarSeer_Results_Summary_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Get startingMonth and finalMonth
    starting_month = quarry.startingMonth
    final_month = quarry.finalMonth


    # Optimisation
    FarSeer_meetAllSales_results_df, FarSeer_meetAllSales_hours_df, FarSeer_meetForcedSales_results_df, FarSeer_meetForcedSales_hours_df, FarSeer_maxGM_results_df, FarSeer_maxGM_hours_df = FarSeer.FarSeer_Opt(quarry)

    # Save
    quarry.FarSeer_meetAllSales_results_df_json = FarSeer_meetAllSales_results_df.to_json(orient='records')
    quarry.FarSeer_meetAllSales_hours_df_json = FarSeer_meetAllSales_hours_df.to_json(orient='records')
    quarry.FarSeer_meetForcedSales_results_df_json = FarSeer_meetForcedSales_results_df.to_json(orient='records')
    quarry.FarSeer_meetForcedSales_hours_df_json = FarSeer_meetForcedSales_hours_df.to_json(orient='records')
    quarry.FarSeer_maxGM_results_df_json = FarSeer_maxGM_results_df.to_json(orient='records')
    quarry.FarSeer_maxGM_hours_df_json = FarSeer_maxGM_hours_df.to_json(orient='records')
    quarry.save()

    # Call FarSeer_Summary_Tables
    summaryTableMeetAllSales, summaryTableMeetForcedSales, summaryTableMaxGM, FarSeer_MeetAllSales_OptimisationSuccess, FarSeer_MeetForcedSales_OptimisationSuccess, FarSeer_MaxGM_OptimisationSuccess = FarSeer.FarSeer_Summary_Tables(FarSeer_meetAllSales_results_df, FarSeer_meetForcedSales_results_df, FarSeer_maxGM_results_df, quarry)


    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
        'summaryTableMeetAllSales': summaryTableMeetAllSales,
        'summaryTableMeetForcedSales': summaryTableMeetForcedSales,
        'summaryTableMaxGM': summaryTableMaxGM,
        'starting_month': starting_month,
        'final_month': final_month,
        'FarSeer_MeetAllSales_OptimisationSuccess': FarSeer_MeetAllSales_OptimisationSuccess,
        'FarSeer_MeetForcedSales_OptimisationSuccess': FarSeer_MeetForcedSales_OptimisationSuccess,
        'FarSeer_MaxGM_OptimisationSuccess': FarSeer_MaxGM_OptimisationSuccess,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/FarSeer_Portal/FarSeer_Results_Summary.html', context)

def FarSeer_Results_Meet_All_Sales_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve dataframes
    FarSeer_meetAllSales_results_df_json = quarry.FarSeer_meetAllSales_results_df_json
    FarSeer_meetAllSales_results_df = pd.read_json(StringIO(FarSeer_meetAllSales_results_df_json), orient='records')
    FarSeer_meetAllSales_hours_df_json = quarry.FarSeer_meetAllSales_hours_df_json
    FarSeer_meetAllSales_hours_df = pd.read_json(StringIO(FarSeer_meetAllSales_hours_df_json), orient='records')
    maxOpeningHours_df_json = quarry.maxOpeningHours_df_json
    maxOpeningHours_df = pd.read_json(StringIO(maxOpeningHours_df_json), orient='records')

    # Format Coefficient
    FarSeer_meetAllSales_hours_df['Coefficient'] = FarSeer_meetAllSales_hours_df['Coefficient'].astype(float).round(2)
    FarSeer_meetAllSales_hours_df['Operating_Hours'] = FarSeer_meetAllSales_hours_df['Operating_Hours'].astype(float).round(1)
    FarSeer_meetAllSales_hours_df['Opening_Hours'] = FarSeer_meetAllSales_hours_df['Opening_Hours'].astype(float).round(1)

    # Get unique_months
    unique_months = FarSeer_meetAllSales_results_df['Month_Name'].unique()

    # Call Charts for FarSeer_meetAllSales_results_df
    dataframe = FarSeer_meetAllSales_results_df
    TO_Chart = FarSeer.FarSeer_TO_Chart(project_data, dataframe)
    VC_Chart = FarSeer.FarSeer_VC_Chart(project_data, dataframe)
    GM_Chart = FarSeer.FarSeer_GM_Chart(project_data, dataframe)
    Stock_Value_Chart = FarSeer.FarSeer_Stock_Value_Chart(project_data,dataframe)


    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
        'unique_months': unique_months,
        'FarSeer_results_df': FarSeer_meetAllSales_results_df,
        'FarSeer_hours_df': FarSeer_meetAllSales_hours_df,
        'maxOpeningHours_df': maxOpeningHours_df,
        'TO_Chart': TO_Chart,
        'VC_Chart': VC_Chart,
        'GM_Chart': GM_Chart,
        'Stock_Value_Chart': Stock_Value_Chart,
        'Download_df': FarSeer_meetAllSales_results_df.to_json(orient='split'),  # Convert DataFrame to JSON
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/FarSeer_Portal/FarSeer_Results_Meet_All_Sales.html', context)

@login_required
def FarSeer_Results_Meet_Forced_Sales_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve dataframes
    FarSeer_meetForcedSales_results_df_json = quarry.FarSeer_meetForcedSales_results_df_json
    FarSeer_meetForcedSales_results_df = pd.read_json(StringIO(FarSeer_meetForcedSales_results_df_json), orient='records')
    FarSeer_meetForcedSales_hours_df_json = quarry.FarSeer_meetForcedSales_hours_df_json
    FarSeer_meetForcedSales_hours_df = pd.read_json(StringIO(FarSeer_meetForcedSales_hours_df_json), orient='records')
    maxOpeningHours_df_json = quarry.maxOpeningHours_df_json
    maxOpeningHours_df = pd.read_json(StringIO(maxOpeningHours_df_json), orient='records')

    # Format Coefficient
    FarSeer_meetForcedSales_hours_df['Coefficient'] = FarSeer_meetForcedSales_hours_df['Coefficient'].astype(float).round(2)
    FarSeer_meetForcedSales_hours_df['Operating_Hours'] = FarSeer_meetForcedSales_hours_df['Operating_Hours'].astype(float).round(1)
    FarSeer_meetForcedSales_hours_df['Opening_Hours'] = FarSeer_meetForcedSales_hours_df['Opening_Hours'].astype(float).round(1)

    # Get unique_months
    unique_months = FarSeer_meetForcedSales_results_df['Month_Name'].unique()

    # Call Charts for FarSeer_meetForcedSales_results_df
    dataframe = FarSeer_meetForcedSales_results_df
    TO_Chart = FarSeer.FarSeer_TO_Chart(project_data, dataframe)
    VC_Chart = FarSeer.FarSeer_VC_Chart(project_data, dataframe)
    GM_Chart = FarSeer.FarSeer_GM_Chart(project_data, dataframe)
    Stock_Value_Chart = FarSeer.FarSeer_Stock_Value_Chart(project_data, dataframe)

    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
        'unique_months': unique_months,
        'FarSeer_results_df': FarSeer_meetForcedSales_results_df,
        'FarSeer_hours_df': FarSeer_meetForcedSales_hours_df,
        'maxOpeningHours_df': maxOpeningHours_df,
        'TO_Chart': TO_Chart,
        'VC_Chart': VC_Chart,
        'GM_Chart': GM_Chart,
        'Stock_Value_Chart': Stock_Value_Chart,
        'Download_df': FarSeer_meetForcedSales_results_df.to_json(orient='split'),  # Convert DataFrame to JSON
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/FarSeer_Portal/FarSeer_Results_Meet_Forced_Sales.html', context)

@login_required
def FarSeer_Results_MaxGM_view(request, project_id, quarry_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve the quarry based on quarry_id
    quarry = get_object_or_404(Quarry, pk=quarry_id)

    # Retrieve dataframes
    FarSeer_maxGM_results_df_json = quarry.FarSeer_maxGM_results_df_json
    FarSeer_maxGM_results_df = pd.read_json(StringIO(FarSeer_maxGM_results_df_json), orient='records')
    FarSeer_maxGM_hours_df_json = quarry.FarSeer_maxGM_hours_df_json
    FarSeer_maxGM_hours_df = pd.read_json(StringIO(FarSeer_maxGM_hours_df_json), orient='records')
    maxOpeningHours_df_json = quarry.maxOpeningHours_df_json
    maxOpeningHours_df = pd.read_json(StringIO(maxOpeningHours_df_json), orient='records')

    # Format Coefficient
    FarSeer_maxGM_hours_df['Coefficient'] = FarSeer_maxGM_hours_df['Coefficient'].astype(float).round(2)
    FarSeer_maxGM_hours_df['Operating_Hours'] = FarSeer_maxGM_hours_df['Operating_Hours'].astype(float).round(1)
    FarSeer_maxGM_hours_df['Opening_Hours'] = FarSeer_maxGM_hours_df['Opening_Hours'].astype(float).round(1)

    # Get unique_months
    unique_months = FarSeer_maxGM_results_df['Month_Name'].unique()

    # Call Charts for FarSeer_maxGM_results_df
    dataframe = FarSeer_maxGM_results_df
    TO_Chart = FarSeer.FarSeer_TO_Chart(project_data, dataframe)
    VC_Chart = FarSeer.FarSeer_VC_Chart(project_data, dataframe)
    GM_Chart = FarSeer.FarSeer_GM_Chart(project_data, dataframe)
    Stock_Value_Chart = FarSeer.FarSeer_Stock_Value_Chart(project_data, dataframe)


    context = {
        'project_data': project_data,
        'quarry': quarry,
        'project_id': project_id,
        'quarry_id': quarry_id,
        'project_name': project_data.project_name,
        'quarry_name': quarry.name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
        'unique_months': unique_months,
        'FarSeer_results_df': FarSeer_maxGM_results_df,
        'FarSeer_hours_df': FarSeer_maxGM_hours_df,
        'maxOpeningHours_df': maxOpeningHours_df,
        'TO_Chart': TO_Chart,
        'VC_Chart': VC_Chart,
        'GM_Chart': GM_Chart,
        'Stock_Value_Chart': Stock_Value_Chart,
        'Download_df': FarSeer_maxGM_results_df.to_json(orient='split'),  # Convert DataFrame to JSON
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/FarSeer_Portal/FarSeer_Results_MaxGM.html', context)


@login_required
def Network_Optimisation_Portal_view(request, project_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    if request.method == 'POST':
        scenario_name = request.POST.get('scenario_name')
        if scenario_name:
            try:
                scenario = Scenario(
                    scenario_name=scenario_name,
                    project=project_data,
                )
                scenario.save()
                return redirect('NO_Scenario_Information', project_id=project_id, scenario_id=scenario.scenario_id)
            except IntegrityError:
                return HttpResponse("Scenario with this name already exists for the project", status=400)
        else:
            return redirect('Network_Optimisation_Portal', project_id=project_id)
    else:
        # Retrieve all scenarios associated with the project
        scenarios = project_data.scenarios.all()


        NO_Baseline_Done = project_data.NO_Baseline_Done
        is_editor = request.user.groups.filter(name='Editors').exists()

        context = {
            'project_id': project_id,
            'project_data': project_data,
            'project_name': project_data.project_name,
            'project_time_period': project_data.project_time_period,
            'is_editor': is_editor,
            'NO_Baseline_Done': NO_Baseline_Done,
            'scenarios': scenarios,
        }

        return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Network_Optimisation_Portal/Network_Optimisation_Portal.html', context)

@login_required
def Network_Optimisation_Clear_Scenario(request, project_id, scenario_id):
    # Retrieve the scenario instance or return 404 if not found
    scenario = get_object_or_404(Scenario, pk=scenario_id, project__project_id=project_id)

    if request.method == 'POST':
        # Delete the scenario if it's a POST request
        scenario.delete()
        # Redirect to the Network Optimisation Portal page for the project
        return redirect('Network_Optimisation_Portal', project_id=project_id)


@login_required
def NO_Baseline_Information_view(request, project_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    # Retrieve the list of quarries
    quarries_list = sorted(project_data.quarries.all(), key=lambda x: x.name)

    # Retrieve dataframes
    distances_df_json = project_data.distances_df_json
    distances_df = pd.read_json(StringIO(distances_df_json), orient='records')
    baseline_infoTable_df_json = project_data.baseline_infoTable_df_json
    baseline_infoTable_df = pd.read_json(StringIO(baseline_infoTable_df_json), orient='records')
    radiusTable_df_json = project_data.radiusTable_df_json
    radiusTable_df = pd.read_json(StringIO(radiusTable_df_json), orient='records')

    # Find the largest value in the Distance column
    max_distance = distances_df['Distance'].max()/1000
    max_distance = math.ceil(max_distance)

    NO_Completion_df = Portal_Completion.NO_Completion(project_data)

    # Check if Baseline can be calculated (data_completion = 100 means ok for baseline calculation)
    data_completion = int(NO_Completion_df['NO_Completion'].mean())


    # Check if baseline_infoTable_df is not empty
    if not baseline_infoTable_df.empty:
        baseline_infoTable_merged_with_NO_Completion_df = pd.merge(baseline_infoTable_df, NO_Completion_df, on="Quarry", how="inner")
    else:
        baseline_infoTable_merged_with_NO_Completion_df = pd.DataFrame()

    # Check if there are existing scenarios for the project
    scenarios_exist = Scenario.objects.filter(project=project_data).exists()  # Boolean

    context = {
        'project_id': project_id,
        'project_data': project_data,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'quarries_list': quarries_list,
        'currency': project_data.currency,
        'NO_Completion_df': NO_Completion_df,
        'max_distance': max_distance,
        'baseline_infoTable_df': baseline_infoTable_df,
        'radiusTable_df': radiusTable_df,
        'baseline_infoTable_merged_with_NO_Completion_df': baseline_infoTable_merged_with_NO_Completion_df,
        'scenarios_exist': scenarios_exist,
        'data_completion': data_completion,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Network_Optimisation_Portal/NO_Baseline_Information.html', context)


@login_required
def NO_Baseline_Information_to_Results_bridge(request, project_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            project_data = get_object_or_404(ProjectData, project_id=project_id)

            # Load json data from request body & extract the data & convert to dataframe
            jsonData = json.loads(request.body)

            # Extract data for infoTable and radiusTable
            baseline_infoTable_data = jsonData.get('infoTable', [])
            radiusTable_data = jsonData.get('radiusTable', [])

            # Convert the JSON data to DataFrames
            baseline_infoTable_df = pd.DataFrame(baseline_infoTable_data)
            radiusTable_df = pd.DataFrame(radiusTable_data)

            # Set the first row as column names and drop the first row
            baseline_infoTable_df.columns = baseline_infoTable_df.iloc[0]
            baseline_infoTable_df = baseline_infoTable_df.drop(0)
            radiusTable_df.columns = radiusTable_df.iloc[0]
            radiusTable_df = radiusTable_df.drop(0)

            # Convert columns to integers
            baseline_infoTable_df['Max_Opening_Hours'] = baseline_infoTable_df['Max_Opening_Hours'].astype(int)
            baseline_infoTable_df['Max_Sales_Volume'] = baseline_infoTable_df['Max_Sales_Volume'].astype(int)
            baseline_infoTable_df['Fixed_Cost'] = baseline_infoTable_df['Fixed_Cost'].astype(int)
            radiusTable_df['From'] = radiusTable_df['From'].astype(int)
            radiusTable_df['To'] = radiusTable_df['To'].astype(int)
            radiusTable_df['Transport_Cost'] = radiusTable_df['Transport_Cost'].astype(float)

            # Call Calculate_Transport_Cost function
            transport_cost_df = Network_Optimisation.Calculate_Transport_Cost_Radius(project_data, radiusTable_df)

            # Save
            project_data.baseline_infoTable_df_json = baseline_infoTable_df.to_json(orient='records')
            project_data.radiusTable_df_json = radiusTable_df.to_json(orient='records')
            project_data.transport_cost_df_json = transport_cost_df.to_json(orient='records')
            project_data.save()

            # Set Max_Opening_Hours_Max_Sales_Volume_df
            Max_Opening_Hours_Max_Sales_Volume_df = baseline_infoTable_df.copy()

            # Get reduced_customers_df and small_customers_df
            reduced_customers_df, small_customers_df = Network_Optimisation.Reduce_Customer_Amount(project_data)

            # Calculate Enough_capacity_df
            Enough_capacity_df = Network_Optimisation.NO_Check_Quarries_Hours_Volumes_Capacity(project_data, Max_Opening_Hours_Max_Sales_Volume_df, reduced_customers_df)


            # Optimisation Baseline
            # Define model
            mod_file = 'Network_Optimisation_Baseline'
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
                dat_file_path = os.path.join(temp_dir, 'Network_Optimisation_Baseline.dat')
                result_file_path = os.path.join(temp_dir, 'Network_Optimisation_Baseline.sol')

                Transport_Cost_per_Customer_df = Network_Optimisation.NO_Create_dat(project_data, dat_file_path, reduced_customers_df, small_customers_df, Max_Opening_Hours_Max_Sales_Volume_df, Enough_capacity_df)
                NO_Baseline_Raw_Results_df = Network_Optimisation.NO_GLPK(mod_file_path, dat_file_path, result_file_path)


            project_data.NO_Baseline_Raw_Results_df_json = NO_Baseline_Raw_Results_df.to_json(orient='records')
            project_data.reduced_customers_df_json = reduced_customers_df.to_json(orient='records')
            project_data.small_customers_df_json = small_customers_df.to_json(orient='records')
            project_data.Transport_Cost_per_Customer_df_json = Transport_Cost_per_Customer_df.to_json(orient='records')
            project_data.NO_Baseline_Done = True
            project_data.save()

            # Check if scenarios exist for the project and delete them if any
            if Scenario.objects.filter(project=project_data).exists():
                scenarios = Scenario.objects.filter(project=project_data)
                for scenario in scenarios:
                    scenario.NO_Scenario_Raw_Results_df_json = '{}'
                    scenario.NO_Scenario_Customer_Results_df_json = '{}'
                    scenario.NO_Scenario_Done = False
                    scenario.save()


            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            print("JSONDecodeError")
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def NO_Baseline_Results_view(request, project_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)

    # Calculate all details per customer
    NO_Baseline_Customer_Results_df, NO_Baseline_Gross_Sales = Network_Optimisation.Extract_Customer_Results_Baseline(project_data)

    # Extract and calculate P&L labels
    (NO_Baseline_Raw_Results_df, Baseline_PL_Items, NO_Baseline_Outlook_df) = Network_Optimisation.Extract_Baseline_Results(project_data, NO_Baseline_Gross_Sales)

    # Generate Plotly Charts
    Plotly_Revenue, Plotly_Gross_Margin = Network_Optimisation.Create_Plotly_Visualization(project_data, NO_Baseline_Customer_Results_df)

    # Format PL Items Dictionaries
    Baseline_PL_Items = Network_Optimisation.Format_PL_Items_Dictionaries(Baseline_PL_Items)

    # Retrieve dataframes
    transport_cost_df_json = project_data.transport_cost_df_json
    transport_cost_df = pd.read_json(StringIO(transport_cost_df_json), orient='records')


    context = {
        'project_id': project_id,
        'project_data': project_data,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
        'NO_Baseline_Raw_Results_df': NO_Baseline_Raw_Results_df.to_json(orient='split'), # Convert DataFrame to JSON
        'Baseline_PL_Items': Baseline_PL_Items,
        'NO_Baseline_Outlook_df': NO_Baseline_Outlook_df,
        'Plotly_Revenue': Plotly_Revenue,
        'Plotly_Gross_Margin': Plotly_Gross_Margin,
        'transport_cost_df': transport_cost_df.to_json(orient='split'), # Convert DataFrame to JSON,
        'NO_Baseline_Customer_Results_df': NO_Baseline_Customer_Results_df.to_json(orient='split'), # Convert DataFrame to JSON,
    }

    # Save all customer details
    project_data.NO_Baseline_Gross_Sales = NO_Baseline_Gross_Sales
    project_data.NO_Baseline_Customer_Results_df_json = NO_Baseline_Customer_Results_df.to_json(orient='records')
    project_data.save()

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Network_Optimisation_Portal/NO_Baseline_Results.html', context)


@login_required
def NO_Scenario_Information_view(request, project_id, scenario_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve scenario based on scenario_id
    scenario = get_object_or_404(Scenario, scenario_id=scenario_id, project=project_data)

    # Retrieve the list of quarries
    quarries_list = sorted(project_data.quarries.all(), key=lambda x: x.name)

    # Retrieve dataframe and information
    scenario_infoTable_df_json = scenario.scenario_infoTable_df_json
    scenario_infoTable_df = pd.read_json(StringIO(scenario_infoTable_df_json), orient='records')
    NO_Scenario_Done = scenario.NO_Scenario_Done
    optimisation_type = scenario.optimisation_type
    NO_Baseline_Done = project_data.NO_Baseline_Done

    context = {
        'project_id': project_id,
        'project_data': project_data,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'quarries_list': quarries_list,
        'currency': project_data.currency,
        'scenario_id': scenario_id,
        'scenario_name': scenario.scenario_name,
        'scenario_infoTable_df': scenario_infoTable_df,
        'NO_Scenario_Done': NO_Scenario_Done,
        'optimisation_type': optimisation_type,
        'NO_Baseline_Done': NO_Baseline_Done,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Network_Optimisation_Portal/NO_Scenario_Information.html', context)


@login_required
def NO_Scenario_Information_to_Results_bridge(request, project_id, scenario_id):
    if request.method == 'POST':
        try:
            # Retrieve project data or return a 404 error if not found
            project_data = get_object_or_404(ProjectData, project_id=project_id)
            # Retrieve scenario based on scenario_id
            scenario = get_object_or_404(Scenario, scenario_id=scenario_id, project=project_data)

            # Load json data from request body & extract the data & convert to dataframe
            jsonData = json.loads(request.body)

            # Extract data for infoTable and radiusTable
            scenario_infoTable_data = jsonData.get('infoTable', [])
            scenario_optimisation_type = jsonData.get('optimisationType')

            # Convert the JSON data to DataFrames
            scenario_infoTable_df = pd.DataFrame(scenario_infoTable_data)

            # Set the first row as column names and drop the first row
            scenario_infoTable_df.columns = scenario_infoTable_df.iloc[0]
            scenario_infoTable_df = scenario_infoTable_df.drop(0)

            # Convert columns to integers
            scenario_infoTable_df['Max_Opening_Hours'] = scenario_infoTable_df['Max_Opening_Hours'].astype(int)
            scenario_infoTable_df['Max_Sales_Volume'] = scenario_infoTable_df['Max_Sales_Volume'].astype(int)
            scenario_infoTable_df['Fixed_Cost'] = scenario_infoTable_df['Fixed_Cost'].astype(int)

            # Save
            scenario.scenario_infoTable_df_json = scenario_infoTable_df.to_json(orient='records')
            scenario.optimisation_type = scenario_optimisation_type
            scenario.save()

            # Retrieve reduced_customers_df and small_customers_df
            reduced_customers_df_json = project_data.reduced_customers_df_json
            reduced_customers_df = pd.read_json(StringIO(reduced_customers_df_json), orient='records')
            small_customers_df_json = project_data.small_customers_df_json
            small_customers_df = pd.read_json(StringIO(small_customers_df_json), orient='records')

            # Set Max_Opening_Hours_Max_Sales_Volume_df
            Max_Opening_Hours_Max_Sales_Volume_df = scenario_infoTable_df.copy()

            # Calculate Enough_capacity_df
            Enough_capacity_df = Network_Optimisation.NO_Check_Quarries_Hours_Volumes_Capacity(project_data, Max_Opening_Hours_Max_Sales_Volume_df, reduced_customers_df)

            if scenario_optimisation_type == 'meeting_all_sales':
                # Optimisation Meeting All Sales
                # Define model
                mod_file = 'Network_Optimisation_Scenario_MeetingAllSales'
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
                    dat_file_path = os.path.join(temp_dir, 'Network_Optimisation_Scenario_MeetingAllSales.dat')
                    result_file_path = os.path.join(temp_dir, 'Network_Optimisation_Scenario_MeetingAllSales.sol')

                    Network_Optimisation.NO_Create_dat(project_data, dat_file_path, reduced_customers_df, small_customers_df, Max_Opening_Hours_Max_Sales_Volume_df, Enough_capacity_df)
                    NO_Scenario_Raw_Results_df = Network_Optimisation.NO_GLPK(mod_file_path, dat_file_path, result_file_path)

                    scenario.NO_Scenario_Raw_Results_df_json = NO_Scenario_Raw_Results_df.to_json(orient='records')
                    scenario.NO_Scenario_Done = True
                    scenario.save()


            else :
                # Optimisation Meeting All Sales to define best routes
                # Define model
                mod_file = 'Network_Optimisation_Scenario_MeetingAllSales'
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
                    dat_file_path = os.path.join(temp_dir, 'Network_Optimisation_Scenario_MeetingAllSales.dat')
                    result_file_path = os.path.join(temp_dir, 'Network_Optimisation_Scenario_MeetingAllSales.sol')

                    Network_Optimisation.NO_Create_dat(project_data, dat_file_path, reduced_customers_df, small_customers_df, Max_Opening_Hours_Max_Sales_Volume_df, Enough_capacity_df)
                    NO_Scenario_Raw_Results_df = Network_Optimisation.NO_GLPK(mod_file_path, dat_file_path, result_file_path)


                # Optimisation Maximizing Gross Margin
                # Define model
                mod_file = 'Network_Optimisation_Scenario_MaximizingGrossMargin'
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
                    dat_file_path = os.path.join(temp_dir, 'Network_Optimisation_Scenario_MaximizingGrossMargin.dat')
                    result_file_path = os.path.join(temp_dir, 'Network_Optimisation_Scenario_MaximizingGrossMargin.sol')

                    Network_Optimisation.NO_Create_MaxGM_dat(project_data, dat_file_path, reduced_customers_df, small_customers_df, NO_Scenario_Raw_Results_df)
                    NO_Scenario_Raw_Results_df = Network_Optimisation.NO_GLPK(mod_file_path, dat_file_path, result_file_path)

                    scenario.NO_Scenario_Raw_Results_df_json = NO_Scenario_Raw_Results_df.to_json(orient='records')
                    scenario.NO_Scenario_Done = True
                    scenario.save()

            # Add a response for the POST request
            return JsonResponse({'success': True})
        except json.JSONDecodeError:
            print("JSONDecodeError")
            return redirect('Error')
        except Exception as e:
            print("Exception:", str(e))
            return redirect('Error')
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=405)


@login_required
def NO_Scenario_Results_view(request, project_id, scenario_id):
    # Retrieve project data based on custom project ID
    project_data = get_object_or_404(ProjectData, project_id=project_id)
    # Retrieve scenario based on scenario_id
    scenario = get_object_or_404(Scenario, scenario_id=scenario_id, project=project_data)
    # Retrieve NO_Baseline_Gross_Sales
    NO_Baseline_Gross_Sales = project_data.NO_Baseline_Gross_Sales

    # Extract Customer Results for the Scenario
    NO_Scenario_Customer_Results_df, NO_Scenario_Gross_Sales = Network_Optimisation.Extract_Customer_Results_Scenario(project_data, scenario)

    # Extract and calculate P&L labels for Baseline
    (NO_Baseline_Raw_Results_df, Baseline_PL_Items, NO_Baseline_Outlook_df) = Network_Optimisation.Extract_Baseline_Results(project_data, NO_Baseline_Gross_Sales)

    # Extract and calculate P&L labels for Scenario - NO_Baseline_Gross_Sales kept as we need it to calculate the gain in transport
    (NO_Scenario_Raw_Results_df, Scenario_PL_Items, NO_Scenario_Outlook_df) = Network_Optimisation.Extract_Scenario_Results(scenario, NO_Scenario_Gross_Sales)

    # Calculate Delta between Scenario and Baseline
    Delta_PL_Items = Network_Optimisation.Calculate_Baseline_Scenario_PL_Delta(Baseline_PL_Items, Scenario_PL_Items)

    # Get Customer_Delta_Baseline_Scenario_df
    Customer_Delta_Baseline_Scenario_df, Customer_Delta_Baseline_Scenario_display = Network_Optimisation.Customer_Delta_Baseline_Scenario(project_data, NO_Scenario_Customer_Results_df)

    # Waterfall Chat with P&L Impacts
    Impact_Breakdown_Chart = Network_Optimisation.Waterfall_Chart_Delta_Scenario(project_data, Delta_PL_Items, Customer_Delta_Baseline_Scenario_df)

    # Format PL Items Dictionaries
    Baseline_PL_Items = Network_Optimisation.Format_PL_Items_Dictionaries(Baseline_PL_Items)
    Scenario_PL_Items = Network_Optimisation.Format_PL_Items_Dictionaries(Scenario_PL_Items)
    Delta_PL_Items = Network_Optimisation.Format_PL_Items_Dictionaries(Delta_PL_Items)

    context = {
        'project_id': project_id,
        'project_data': project_data,
        'project_name': project_data.project_name,
        'project_time_period': project_data.project_time_period,
        'currency': project_data.currency,
        'scenario': scenario,
        'scenario_id': scenario_id,
        'scenario_name': scenario.scenario_name,

        'Baseline_PL_Items': Baseline_PL_Items,

        'Impact_Breakdown_Chart': Impact_Breakdown_Chart,

        'NO_Scenario_Raw_Results_df': NO_Scenario_Raw_Results_df.to_json(orient='split'), # Convert DataFrame to JSON
        'NO_Scenario_Customer_Results_df': NO_Scenario_Customer_Results_df.to_json(orient='split'), # Convert DataFrame to JSON
        'Scenario_PL_Items': Scenario_PL_Items,

        'NO_Scenario_Outlook_df': NO_Scenario_Outlook_df,

        'Delta_PL_Items': Delta_PL_Items,

        'Customer_Delta_Baseline_Scenario_df': Customer_Delta_Baseline_Scenario_df.to_json(orient='split'), # Convert DataFrame to JSON,
        'Customer_Delta_Baseline_Scenario_display': Customer_Delta_Baseline_Scenario_display,
    }

    return render(request, 'QuarryIQ/Existing_Projects/Data_Portal/Network_Optimisation_Portal/NO_Scenario_Results.html', context)
