# Databricks notebook source
# DBTITLE 1,Mount Dataset 
dbutils.fs.mount(
    source='wasbs://project-movies-data@moviesdatasa.blob.core.windows.net',
    mount_point='/mnt/project-movies-data',
    extra_configs = {'fs.azure.account.key.moviesdatasa.blob.core.windows.net': dbutils.secrets.get('projectmoviesscope', 'storageAccountKey')}

