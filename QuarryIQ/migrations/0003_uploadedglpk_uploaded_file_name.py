# Generated by Django 5.0.7 on 2024-07-23 16:10

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('QuarryIQ', '0002_uploadedglpk'),
    ]

    operations = [
        migrations.AddField(
            model_name='uploadedglpk',
            name='uploaded_file_name',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]