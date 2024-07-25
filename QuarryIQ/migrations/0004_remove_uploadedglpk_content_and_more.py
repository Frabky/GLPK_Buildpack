# Generated by Django 5.0.7 on 2024-07-23 16:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('QuarryIQ', '0003_uploadedglpk_uploaded_file_name'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='uploadedglpk',
            name='content',
        ),
        migrations.AddField(
            model_name='uploadedglpk',
            name='encrypted_content',
            field=models.BinaryField(default=b''),
        ),
    ]