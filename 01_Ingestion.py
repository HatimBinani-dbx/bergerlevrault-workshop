# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # 📥 Étape 2 : Ingestion des données avec Autoloader
# MAGIC
# MAGIC **Autoloader** est une fonctionnalité Databricks qui permet de charger automatiquement des fichiers dès qu'ils arrivent dans un répertoire. C'est parfait pour traiter des données en continu !
# MAGIC
# MAGIC ## 🎯 Ce que nous allons faire :
# MAGIC * Configurer Autoloader pour lire nos fichiers CSV
# MAGIC * Créer des tables Delta pour stocker nos données
# MAGIC * Vérifier que l'ingestion fonctionne correctement

# COMMAND ----------

catalog = 'gmao_catalog'
schema = 'gmao_schema'
volume = 'gmao_volume'
data_path = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

# DBTITLE 1,🔧 Configuration d'Autoloader pour les équipements
# Configuration d'Autoloader pour charger les données d'équipements
# Autoloader surveille automatiquement le répertoire et charge les nouveaux fichiers

# Lecture avec Autoloader
equipments_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{data_path}/schemas/equipments")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(data_path + "/equipments")
)

print("✅ Configuration Autoloader pour les équipements terminée")
print(f"📊 Schéma détecté : {len(equipments_df.columns)} colonnes")
print("🔄 Autoloader surveille maintenant le répertoire pour les nouveaux fichiers")

# COMMAND ----------

# DBTITLE 1,💾 Sauvegarde des équipements en table Delta
# Sauvegarde des données d'équipements en format Delta
# Delta Lake offre des fonctionnalités avancées : versioning, time travel, ACID transactions

equipments_table_name = f"{catalog}.{schema}.equipments"

# Écriture en streaming vers une table Delta Unity Catalog
equipments_query = (equipments_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{data_path}/checkpoints/equipments")
    .trigger(once=True)
    .toTable(equipments_table_name)
)

# Attendre la fin du traitement
equipments_query.awaitTermination()

print("✅ Table Delta 'equipments' créée avec succès")
print(f"📍 Emplacement : {equipments_table_name}")
print("🎯 Les données sont maintenant disponibles pour l'analyse !")

# COMMAND ----------

equipments_table = spark.read.table(f"{catalog}.{schema}.equipments")
display(equipments_table)

# COMMAND ----------

# DBTITLE 1,🔧 Configuration d'Autoloader pour les ordres de travail
from pyspark.sql.types import *

# Configuration d'Autoloader pour les ordres de travail
work_orders_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{data_path}/schemas/work_orders")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(data_path + "/work_orders")
)

print("✅ Configuration Autoloader pour les ordres de travail terminée")
print(f"📊 Schéma détecté : {len(work_orders_df.columns)} colonnes")

# COMMAND ----------

# DBTITLE 1,💾 Sauvegarde des ordres de travail en table Delta
# Sauvegarde des ordres de travail en format Delta

work_orders_table_name = f"{catalog}.{schema}.work_orders"

work_orders_query = (work_orders_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{data_path}/checkpoints/work_orders")
    .trigger(once=True)
    .toTable(work_orders_table_name)
)

work_orders_query.awaitTermination()

print("✅ Table Delta 'work_orders' créée avec succès")
print(f"📍 Emplacement : {work_orders_table_name}")

# COMMAND ----------

work_orders_table = spark.read.table(f"{catalog}.{schema}.work_orders")
display(work_orders_table)

# COMMAND ----------

# DBTITLE 1,🔧 Configuration d'Autoloader pour les interventions
# Configuration d'Autoloader pour les interventions

interventions_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{data_path}/schemas/interventions")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(data_path + "/interventions")
)

print("✅ Configuration Autoloader pour les interventions terminée")
print(f"📊 Schéma détecté : {len(interventions_df.columns)} colonnes")

# COMMAND ----------

# DBTITLE 1,💾 Sauvegarde des interventions en table Delta
# Sauvegarde des interventions en format Delta

interventions_table_name = f"{catalog}.{schema}.interventions"

interventions_query = (interventions_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{data_path}/checkpoints/interventions")
    .trigger(once=True)
    .toTable(interventions_table_name)
)

interventions_query.awaitTermination()

print("✅ Table Delta 'interventions' créée avec succès")
print(f"📍 Emplacement : {interventions_table_name}")

# COMMAND ----------

interventions_table = spark.read.table(f"{catalog}.{schema}.interventions")
display(interventions_table)

# COMMAND ----------

# DBTITLE 1,✅ Vérification de l'ingestion
# Vérification que toutes les tables ont été créées correctement

# Lecture des tables Delta créées
print("\n🎉 Toutes les données GMAO ont été ingérées avec succès !")
print("📊 Résumé de l'ingestion :")
print(f"  🏭 Équipements : {equipments_table.count()} lignes")
print(f"  📋 Ordres de travail : {work_orders_table.count()} lignes")
print(f"  🔧 Interventions : {interventions_table.count()} lignes")

print("\n✅ Ingestion terminée avec succès !")
print("🚀 Prêt pour l'exploration des données...")