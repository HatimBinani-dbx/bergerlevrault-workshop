# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # 🔍 Étape 3 : Exploration des données GMAO
# MAGIC
# MAGIC Maintenant que nos données sont ingérées, explorons-les pour mieux comprendre notre parc d'équipements et nos activités de maintenance.
# MAGIC
# MAGIC ## 🎯 Ce que nous allons découvrir :
# MAGIC * La structure et la qualité de nos données
# MAGIC * La répartition des équipements par type et localisation
# MAGIC * L'état des ordres de travail
# MAGIC * Les tendances de maintenance

# COMMAND ----------

catalog = 'gmao_catalog'
schema = 'gmao_schema'
volume = 'gmao_volume'
data_path = f"/Volumes/{catalog}/{schema}/{volume}"

equipments_table = spark.read.table(f"{catalog}.{schema}.equipments")
work_orders_table = spark.read.table(f"{catalog}.{schema}.work_orders")
interventions_table = spark.read.table(f"{catalog}.{schema}.interventions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Equipements

# COMMAND ----------

# DBTITLE 1,📊 Vue d'ensemble des équipements
from pyspark.sql.functions import desc

# Exploration des données d'équipements
print("🏭 ANALYSE DES ÉQUIPEMENTS")
print("=" * 40)

# Affichage des premières lignes
print("📋 Aperçu des données :")
display(equipments_table.limit(5))

# Statistiques générales
print(f"\n📊 Nombre total d'équipements : {equipments_table.count()}")
print(f"📊 Nombre de colonnes : {len(equipments_table.columns)}")

# Répartition par type d'équipement
print("\n🔧 Répartition par type d'équipement :")
equipment_types_count = equipments_table.groupBy("equipment_type").count().orderBy(desc("count"))
display(equipment_types_count)

# COMMAND ----------

# DBTITLE 1,📍 Analyse de la localisation des équipements
# Analyse de la répartition géographique des équipements
import matplotlib.pyplot as plt

print("📍 ANALYSE DE LOCALISATION")
print("=" * 35)

# Répartition par localisation
location_stats = equipments_table.groupBy("location").count().orderBy(desc("count")).toPandas()

# Graphique en barres
plt.figure(figsize=(12, 6))
plt.bar(location_stats['location'], location_stats['count'], color='steelblue')
plt.title('Répartition des équipements par localisation', fontsize=14, fontweight='bold')
plt.xlabel('Localisation')
plt.ylabel('Nombre d\'\u00e9quipements')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

print(f"\n📍 Nombre de localisations différentes : {location_stats.shape[0]}")
print(f"🎯 Localisation avec le plus d'équipements : {location_stats.iloc[0]['location']} ({location_stats.iloc[0]['count']} équipements)")

# COMMAND ----------

# DBTITLE 1,⚠️ Analyse de la criticité et du statut
# Analyse de la criticité et du statut des équipements
print("⚠️ ANALYSE DE CRITICITÉ ET STATUT")
print("=" * 40)

# Répartition par criticité
criticality_stats = equipments_table.groupBy("criticality").count().orderBy(desc("count"))
print("📊 Répartition par criticité :")
display(criticality_stats)

# Répartition par statut
status_stats = equipments_table.groupBy("status").count().orderBy(desc("count"))
print("\n🔄 Répartition par statut :")
display(status_stats)

# Analyse croisée criticité vs statut
print("\n🔍 Analyse croisée criticité vs statut :")
cross_analysis = equipments_table.groupBy("criticality", "status").count().orderBy("criticality", desc("count"))
display(cross_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Work Orders

# COMMAND ----------

# DBTITLE 1,📋 Exploration des ordres de travail
# Exploration des ordres de travail
print("📋 ANALYSE DES ORDRES DE TRAVAIL")
print("=" * 40)

# Vue d'ensemble
print("📋 Aperçu des ordres de travail :")
display(work_orders_table.limit(5))

print(f"\n📊 Nombre total d'ordres de travail : {work_orders_table.count()}")

# Répartition par type d'ordre de travail
print("\n🔧 Répartition par type d'ordre de travail :")
work_order_types_count = work_orders_table.groupBy("work_order_type").count().orderBy(desc("count"))
display(work_order_types_count)

# Répartition par statut
print("\n🔄 Répartition par statut :")
work_order_status_count = work_orders_table.groupBy("status").count().orderBy(desc("count"))
display(work_order_status_count)

# COMMAND ----------

# DBTITLE 1,📈 Analyse temporelle des ordres de travail
from pyspark.sql.functions import col, date_format, weekofyear

# Analyse temporelle des ordres de travail
print("📈 ANALYSE TEMPORELLE")
print("=" * 25)

# Ajout de colonnes temporelles pour l'analyse
work_orders_with_time = work_orders_table.withColumn(
    "creation_month", date_format(col("creation_date"), "yyyy-MM")
).withColumn(
    "creation_week", weekofyear(col("creation_date"))
)

# Tendance mensuelle de création d'ordres de travail
monthly_trend = work_orders_with_time.groupBy("creation_month").count().orderBy("creation_month")
print("📅 Tendance mensuelle de création d'ordres de travail :")
display(monthly_trend)

# Graphique de tendance
monthly_data = monthly_trend.toPandas()
plt.figure(figsize=(12, 6))
plt.plot(monthly_data['creation_month'], monthly_data['count'], marker='o', linewidth=2, markersize=8)
plt.title('Tendance mensuelle des ordres de travail', fontsize=14, fontweight='bold')
plt.xlabel('Mois')
plt.ylabel('Nombre d\'ordres de travail')
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

print(f"\n📊 Mois le plus chargé : {monthly_data.loc[monthly_data['count'].idxmax(), 'creation_month']} ({monthly_data['count'].max()} ordres)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Interventions

# COMMAND ----------

# DBTITLE 1,🔧 Exploration des interventions
from pyspark.sql.functions import avg, min, max, stddev
# Exploration des interventions
print("🔧 ANALYSE DES INTERVENTIONS")
print("=" * 35)

# Vue d'ensemble
print("📋 Aperçu des interventions :")
display(interventions_table.limit(5))

print(f"\n📊 Nombre total d'interventions : {interventions_table.count()}")

# Répartition par type d'intervention
print("\n🔧 Répartition par type d'intervention :")
intervention_types_count = interventions_table.groupBy("intervention_type").count().orderBy(desc("count"))
display(intervention_types_count)

# Statistiques sur les durées
print("\n⏱️ Statistiques sur les durées d'intervention :")

# Statistiques sur les durées
duration_stats = interventions_table.select(

    avg("duration_hours").alias("duree_moyenne"),
    min("duration_hours").alias("duree_min"),
    max("duration_hours").alias("duree_max"),
    stddev("duration_hours").alias("ecart_type")
)
display(duration_stats)

# COMMAND ----------

# DBTITLE 1,💰 Analyse des coûts de maintenance
from pyspark.sql.functions import sum, avg, count, col, desc

# Analyse des coûts de maintenance
print("💰 ANALYSE DES COÛTS")
print("=" * 25)

# Statistiques générales des coûts
cost_stats = interventions_table.select(

    sum("parts_cost").alias("cout_total_pieces"),
    sum("labor_cost").alias("cout_total_main_oeuvre"),
    avg("parts_cost").alias("cout_moyen_pieces"),
    avg("labor_cost").alias("cout_moyen_main_oeuvre")
)

print("📊 Statistiques des coûts :")
display(cost_stats)

# Coût total par intervention
interventions_with_total_cost = interventions_table.withColumn(
    "total_cost", (col("parts_cost").cast("float")) + (col("labor_cost").cast("float"))
)

# Top 10 des interventions les plus coûteuses
print("\n💸 Top 10 des interventions les plus coûteuses :")
top_costly_interventions = interventions_with_total_cost.select(
    "intervention_id", "intervention_type", "parts_cost", "labor_cost", "total_cost"
).orderBy(desc("total_cost")).limit(10)
display(top_costly_interventions)

# Répartition des coûts par type d'intervention
print("\n📈 Coût moyen par type d'intervention :")
cost_by_type = interventions_with_total_cost.groupBy("intervention_type").agg(
    avg("total_cost").alias("cout_moyen"),
    count("*").alias("nombre_interventions")
).orderBy(desc("cout_moyen"))
display(cost_by_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 ?

# COMMAND ----------

