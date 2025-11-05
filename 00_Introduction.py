# Databricks notebook source
# MAGIC %md
# MAGIC # 🏭 Atelier Databricks - Berger Levrault
# MAGIC
# MAGIC ## 🎯 Objectifs de l'atelier
# MAGIC
# MAGIC Cet atelier vous permettra de découvrir les fonctionnalités de base de Databricks à travers un cas d'usage concret de **GMAO (Gestion de Maintenance Assistée par Ordinateur)**.
# MAGIC
# MAGIC ### Ce que vous allez apprendre :
# MAGIC
# MAGIC * **📥 Ingestion de données** : Comment charger des fichiers CSV avec Autoloader
# MAGIC * **🔍 Exploration de données** : Analyser et comprendre vos données de maintenance
# MAGIC * **🔧 Transformation de données** : Nettoyer et préparer les données pour l'analyse
# MAGIC * **📊 Calcul d'indicateurs** : Créer des KPI de maintenance essentiels
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Scénario de l'atelier
# MAGIC
# MAGIC Vous êtes responsable de la maintenance dans une entreprise industrielle. Vous disposez de données sur :
# MAGIC * Les **équipements** de votre parc machine
# MAGIC * Les **ordres de travail** de maintenance
# MAGIC * Les **interventions** réalisées par les techniciens
# MAGIC
# MAGIC Votre mission : analyser ces données pour optimiser la maintenance et calculer des indicateurs clés.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Prêt à commencer ?
# MAGIC
# MAGIC Suivez les étapes ci-dessous dans l'ordre. Chaque cellule contient des explications détaillées pour vous guider.

# COMMAND ----------

# DBTITLE 1,📁 Répertoire de données
# Définition du chemin de travail
workshop_path = "/Workspace/Users/" + spark.sql("SELECT current_user()").collect()[0][0] + "/bergerlevrault-workshop"
data_path = workshop_path + "/data"
print(f"📁 Répertoire des données : {data_path}")

# COMMAND ----------

# DBTITLE 1,Creation du catalog/schema/volume
# Création du catalog, du schema et du volume Unity Catalog
spark.sql("CREATE CATALOG IF NOT EXISTS gmao_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS gmao_catalog.gmao_schema")
spark.sql("""
    CREATE VOLUME IF NOT EXISTS gmao_catalog.gmao_schema.gmao_volume
    COMMENT 'Volume pour les données GMAO'
""")
print(f"📁 Catalog : gmao_catalog")
print(f"📁 Schema : gmao_schema")
print(f"📁 Volume : gmao_volume")

# COMMAND ----------

# DBTITLE 1,Copie des données dans le volume
# Création des sous-dossiers dans le volume
dbutils.fs.mkdirs("/Volumes/gmao_catalog/gmao_schema/gmao_volume/equipments")
dbutils.fs.mkdirs("/Volumes/gmao_catalog/gmao_schema/gmao_volume/work_orders")
dbutils.fs.mkdirs("/Volumes/gmao_catalog/gmao_schema/gmao_volume/interventions")

# Copie des données vers le volume Unity Catalog
all_generated_files = dbutils.fs.ls(data_path)
for f in all_generated_files:
    name = f.name
    if name.startswith("equipments") and name.endswith('.csv'):
        target = f"/Volumes/gmao_catalog/gmao_schema/gmao_volume/equipments/{name}"
        dbutils.fs.cp(f.path, target)
        print(f"📤 Copié: {name} -> {target}")
    elif name.startswith("work_orders") and name.endswith('.csv'):
        target = f"/Volumes/gmao_catalog/gmao_schema/gmao_volume/work_orders/{name}"
        dbutils.fs.cp(f.path, target)
        print(f"📤 Copié: {name} -> {target}")
    elif name.startswith("interventions") and name.endswith('.csv'):
        target = f"/Volumes/gmao_catalog/gmao_schema/gmao_volume/interventions/{name}"
        dbutils.fs.cp(f.path, target)
        print(f"📤 Copié: {name} -> {target}")

# Vérification du contenu du volume et des sous-dossiers
for folder in ["equipments", "work_orders", "interventions"]:
    print(f"\n📁 Contenu du dossier {folder}:")
    files = dbutils.fs.ls(f"/Volumes/gmao_catalog/gmao_schema/gmao_volume/{folder}")
    for file in files:
        print(f"  📄 {file.name} ({file.size} bytes)")
