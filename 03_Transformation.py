# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # 🔧 Étape 4 : Transformation et nettoyage des données
# MAGIC
# MAGIC Maintenant que nous avons exploré nos données, nous allons les nettoyer et les transformer pour faciliter l'analyse et le calcul d'indicateurs.
# MAGIC
# MAGIC ## 🎯 Objectifs de cette étape :
# MAGIC * Nettoyer les données (valeurs manquantes, formats)
# MAGIC * Créer des colonnes calculées utiles
# MAGIC * Joindre les tables pour une vue unifiée
# MAGIC * Préparer les données pour le calcul des KPI

# COMMAND ----------

catalog = 'gmao_catalog'
schema = 'gmao_schema'
volume = 'gmao_volume'
data_path = f"/Volumes/{catalog}/{schema}/{volume}"

equipments_table = spark.read.table(f"{catalog}.{schema}.equipments")
work_orders_table = spark.read.table(f"{catalog}.{schema}.work_orders")
interventions_table = spark.read.table(f"{catalog}.{schema}.interventions")

# COMMAND ----------

# DBTITLE 1,🧹 Nettoyage des données d'équipements
from pyspark.sql.functions import col, when, round, datediff, current_date

# Nettoyage et enrichissement des données d'équipements

print("🧹 NETTOYAGE DES DONNÉES D'ÉQUIPEMENTS")
print("=" * 45)

# Vérification des valeurs manquantes
print("🔍 Vérification des valeurs manquantes :")
for column in equipments_table.columns:
    null_count = equipments_table.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count} valeurs manquantes")

# Enrichissement avec des colonnes calculées
equipments_clean = equipments_table.withColumn(
    "age_years", 
    round(datediff(current_date(), col("installation_date")) / 365.25, 1)
).withColumn(
    "age_category",
    when(col("age_years") < 2, "Neuf")
    .when(col("age_years") < 5, "Récent")
    .when(col("age_years") < 10, "Mature")
    .otherwise("Ancien")
).withColumn(
    "criticality_score",
    when(col("criticality") == "Critique", 3)
    .when(col("criticality") == "Important", 2)
    .otherwise(1)
)

print("\n✅ Données d'équipements nettoyées et enrichies")
print("📊 Nouvelles colonnes ajoutées : age_years, age_category, criticality_score")

# Aperçu des données enrichies
print("\n📋 Aperçu des données enrichies :")
display(equipments_clean.select("equipment_id", "equipment_name", "age_years", "age_category", "criticality_score").limit(5))

# COMMAND ----------

# DBTITLE 1,📋 Nettoyage des ordres de travail
from pyspark.sql.functions import avg, sum, count

# Nettoyage et enrichissement des ordres de travail
print("📋 NETTOYAGE DES ORDRES DE TRAVAIL")
print("=" * 40)

# Calcul des durées et des retards
work_orders_clean = work_orders_table.withColumn(
    "planned_duration_days",
    datediff(col("planned_date"), col("creation_date"))
).withColumn(
    "actual_duration_days",
    when(col("completion_date").isNotNull(), 
         datediff(col("completion_date"), col("creation_date"))
    ).otherwise(None)
).withColumn(
    "delay_days",
    when(col("completion_date").isNotNull(),
         datediff(col("completion_date"), col("planned_date"))
    ).otherwise(None)
).withColumn(
    "is_delayed",
    when(col("delay_days") > 0, True).otherwise(False)
).withColumn(
    "priority_score",
    when(col("priority") == "Très urgent", 4)
    .when(col("priority") == "Urgent", 3)
    .when(col("priority") == "Normal", 2)
    .otherwise(1)
).withColumn(
    "hours_variance",
    when(col("actual_hours").isNotNull() & col("estimated_hours").isNotNull(),
         col("actual_hours").cast("double") - col("estimated_hours").cast("double")
    ).otherwise(None)
)

print("✅ Données d'ordres de travail nettoyées et enrichies")
print("📊 Nouvelles colonnes : planned_duration_days, delay_days, is_delayed, priority_score, hours_variance")

# Statistiques sur les retards
delay_stats = work_orders_clean.filter(col("completion_date").isNotNull()).agg(
    avg("delay_days").alias("retard_moyen_jours"),
    sum(when(col("is_delayed"), 1).otherwise(0)).alias("nombre_retards"),
    count("*").alias("total_termines")
)

print("\n📈 Statistiques sur les retards :")
display(delay_stats)

# COMMAND ----------

# DBTITLE 1,🔧 Nettoyage des interventions
from pyspark.sql.functions import min, max

# Nettoyage et enrichissement des interventions
print("🔧 NETTOYAGE DES INTERVENTIONS")
print("=" * 35)

# Enrichissement des données d'interventions
interventions_clean = interventions_table.withColumn(
    "total_cost",
    col("parts_cost").cast("double") + col("labor_cost").cast("double")
).withColumn(
    "cost_category",
    when(col("total_cost") < 100, "Faible")
    .when(col("total_cost") < 300, "Moyen")
    .otherwise("Elevé")
).withColumn(
    "duration_category",
    when(col("duration_hours").cast("double") < 2, "Courte")
    .when(col("duration_hours").cast("double") < 6, "Moyenne")
    .otherwise("Longue")
).withColumn(
    "has_parts",
    when(col("parts_cost").cast("double") > 0, True).otherwise(False)
).withColumn(
    "cost_per_hour",
    when(col("duration_hours").cast("double") > 0, 
         round(col("total_cost") / col("duration_hours").cast("double"), 2)
    ).otherwise(None)
)

print("✅ Données d'interventions nettoyées et enrichies")
print("📊 Nouvelles colonnes : total_cost, cost_category, duration_category, has_parts, cost_per_hour")

# Statistiques sur les coûts par heure
cost_per_hour_stats = interventions_clean.filter(col("cost_per_hour").isNotNull()).agg(
    avg("cost_per_hour").alias("cout_moyen_par_heure"),
    min("cost_per_hour").alias("cout_min_par_heure"),
    max("cost_per_hour").alias("cout_max_par_heure")
)

print("\n💰 Statistiques coût par heure :")
display(cost_per_hour_stats)

# COMMAND ----------

# DBTITLE 1,🔗 Jointure des tables - Vue unifiée
# Création d'une vue unifiée en joignant toutes les tables
print("🔗 CRÉATION D'UNE VUE UNIFIÉE")
print("=" * 35)

# Jointure équipements + ordres de travail avec alias pour éviter l'ambiguïté
equipments_workorders = equipments_clean.alias("eq").join(
    work_orders_clean.alias("wo"),
    "equipment_id",
    "inner"
).select(
    # Colonnes équipements
    col("eq.equipment_id"),
    col("eq.equipment_name"),
    col("eq.equipment_type"),
    col("eq.location"),
    col("eq.manufacturer"),
    col("eq.criticality"),
    col("eq.age_years"),
    col("eq.age_category"),
    col("eq.criticality_score"),
    
    # Colonnes ordres de travail
    col("wo.work_order_id"),
    col("wo.work_order_type"),
    col("wo.status"),
    col("wo.priority"),
    col("wo.creation_date"),
    col("wo.completion_date"),
    col("wo.assigned_technician"),
    col("wo.delay_days"),
    col("wo.is_delayed"),
    col("wo.hours_variance")
)

# Jointure avec les interventions
full_maintenance_view = equipments_workorders.join(
    interventions_clean,
    "work_order_id",
    "left"
).select(
    # Toutes les colonnes précédentes
    col("equipment_id"),
    col("equipment_name"),
    col("equipment_type"),
    col("location"),
    col("manufacturer"),
    col("criticality"),
    col("age_years"),
    col("criticality_score"),
    col("work_order_id"),
    col("work_order_type"),
    col("status"),
    col("priority"),
    col("creation_date"),
    col("completion_date"),
    col("assigned_technician"),
    col("delay_days"),
    col("is_delayed"),
    
    # Colonnes interventions
    col("intervention_id"),
    col("intervention_type"),
    col("duration_hours"),
    col("total_cost"),
    col("cost_category"),
    col("has_parts")
)

print(f"✅ Vue unifiée créée avec {full_maintenance_view.count()} lignes")
print(f"📊 Nombre de colonnes : {len(full_maintenance_view.columns)}")

# Aperçu de la vue unifiée
print("\n📋 Aperçu de la vue unifiée :")
display(full_maintenance_view.limit(5))

# COMMAND ----------

# DBTITLE 1,💾 Sauvegarde de la vue unifiée
# Sauvegarde de la vue unifiée en table Delta
print("💾 SAUVEGARDE DE LA VUE UNIFIÉE")
print("=" * 35)

unified_table_path = f"{catalog}.{schema}.maintenance_unified"

# Sauvegarde en format Delta
full_maintenance_view.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(unified_table_path)

print(f"✅ Vue unifiée sauvegardée : {unified_table_path}")
print("✅ Vue temporaire 'maintenance_unified' créée")
print("🚀 Données prêtes pour le calcul des KPI !")