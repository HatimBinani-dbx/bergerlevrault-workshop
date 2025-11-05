# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # 📊 Étape 4 : Calcul des KPI de maintenance
# MAGIC
# MAGIC Maintenant que nos données sont nettoyées et unifiées, calculons les indicateurs clés de performance (KPI) essentiels pour le pilotage de la maintenance.
# MAGIC
# MAGIC ## 🎯 KPI que nous allons calculer :
# MAGIC * **Taux de disponibilité** des équipements
# MAGIC * **MTBF** (Mean Time Between Failures)
# MAGIC * **MTTR** (Mean Time To Repair)
# MAGIC * **Coûts de maintenance** par équipement et par type
# MAGIC * **Performance des techniciens**
# MAGIC * **Respect des délais** de maintenance

# COMMAND ----------

catalog = 'gmao_catalog'
schema = 'gmao_schema'
volume = 'gmao_volume'
data_path = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
print(f"✅ Using catalog: {catalog}")
print(f"✅ Using schema: {schema}")

# COMMAND ----------

# DBTITLE 1,📈 KPI 1 : Taux de respect des délais
import matplotlib.pyplot as plt# Calcul du taux de respect des délais

print("📈 KPI 1 : TAUX DE RESPECT DES DÉLAIS")
print("=" * 45)

# Calcul global du taux de respect des délais
delay_kpi = spark.sql("""
    SELECT 
        COUNT(*) as total_ordres_termines,
        SUM(CASE WHEN is_delayed = false THEN 1 ELSE 0 END) as ordres_dans_les_temps,
        ROUND(SUM(CASE WHEN is_delayed = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_respect_delais,
        ROUND(AVG(delay_days), 2) as retard_moyen_jours
    FROM maintenance_unified 
    WHERE status = 'Terminé' AND completion_date IS NOT NULL
""")

print("🎯 Taux de respect des délais global :")
display(delay_kpi)

# Taux de respect par priorité
delay_by_priority = spark.sql("""
    SELECT 
        priority,
        COUNT(*) as total_ordres,
        ROUND(SUM(CASE WHEN is_delayed = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_respect_delais
    FROM maintenance_unified 
    WHERE status = 'Terminé' AND completion_date IS NOT NULL
    GROUP BY priority
    ORDER BY taux_respect_delais DESC
""")

print("\n📈 Taux de respect par priorité :")
display(delay_by_priority)

# Graphique
delay_data = delay_by_priority.toPandas()
plt.figure(figsize=(10, 6))
plt.bar(delay_data['priority'], delay_data['taux_respect_delais'], color=['red', 'orange', 'yellow', 'green'])
plt.title('Taux de respect des délais par priorité', fontsize=14, fontweight='bold')
plt.xlabel('Priorité')
plt.ylabel('Taux de respect (%)')
plt.ylim(0, 100)
for i, v in enumerate(delay_data['taux_respect_delais']):
    plt.text(i, v + 1, f'{v}%', ha='center', fontweight='bold')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,⏱️ KPI 2 : MTTR (Mean Time To Repair)
# Calcul du MTTR (Mean Time To Repair)
print("⏱️ KPI 2 : MTTR (MEAN TIME TO REPAIR)")
print("=" * 45)

# MTTR global
mttr_global = spark.sql("""
    SELECT 
        ROUND(AVG(duration_hours), 2) as mttr_heures,
        ROUND(AVG(duration_hours) / 24, 2) as mttr_jours,
        COUNT(*) as nombre_interventions
    FROM maintenance_unified 
    WHERE intervention_id IS NOT NULL AND duration_hours IS NOT NULL
""")

print("🎯 MTTR global :")
display(mttr_global)

# MTTR par type d'équipement
mttr_by_equipment_type = spark.sql("""
    SELECT 
        equipment_type,
        ROUND(AVG(duration_hours), 2) as mttr_heures,
        COUNT(*) as nombre_interventions
    FROM maintenance_unified 
    WHERE intervention_id IS NOT NULL AND duration_hours IS NOT NULL
    GROUP BY equipment_type
    ORDER BY mttr_heures DESC
""")

print("\n🔧 MTTR par type d'équipement :")
display(mttr_by_equipment_type)

# MTTR par type d'intervention
mttr_by_intervention_type = spark.sql("""
    SELECT 
        intervention_type,
        ROUND(AVG(duration_hours), 2) as mttr_heures,
        COUNT(*) as nombre_interventions
    FROM maintenance_unified 
    WHERE intervention_id IS NOT NULL AND duration_hours IS NOT NULL
    GROUP BY intervention_type
    ORDER BY mttr_heures DESC
""")

print("\n🔍 MTTR par type d'intervention :")
display(mttr_by_intervention_type)

# COMMAND ----------

# DBTITLE 1,💰 KPI 3 : Coûts de maintenance
# Analyse des coûts de maintenance
print("💰 KPI 3 : COÛTS DE MAINTENANCE")
print("=" * 40)

# Coûts globaux
cost_kpi = spark.sql("""
    SELECT 
        ROUND(SUM(total_cost), 2) as cout_total,
        ROUND(AVG(total_cost), 2) as cout_moyen_intervention,
        COUNT(*) as nombre_interventions,
        ROUND(SUM(total_cost) / COUNT(DISTINCT equipment_id), 2) as cout_moyen_par_equipement
    FROM maintenance_unified 
    WHERE intervention_id IS NOT NULL AND total_cost IS NOT NULL
""")

print("🎯 Coûts globaux de maintenance :")
display(cost_kpi)

# Coût par type d'équipement
cost_by_equipment_type = spark.sql("""
    SELECT 
        equipment_type,
        ROUND(SUM(total_cost), 2) as cout_total,
        ROUND(AVG(total_cost), 2) as cout_moyen,
        COUNT(*) as nombre_interventions
    FROM maintenance_unified 
    WHERE intervention_id IS NOT NULL AND total_cost IS NOT NULL
    GROUP BY equipment_type
    ORDER BY cout_total DESC
""")

print("\n🏭 Coût par type d'équipement :")
display(cost_by_equipment_type)

# Coût par criticité
cost_by_criticality = spark.sql("""
    SELECT 
        criticality,
        ROUND(SUM(total_cost), 2) as cout_total,
        ROUND(AVG(total_cost), 2) as cout_moyen,
        COUNT(*) as nombre_interventions
    FROM maintenance_unified 
    WHERE intervention_id IS NOT NULL AND total_cost IS NOT NULL
    GROUP BY criticality
    ORDER BY cout_total DESC
""")

print("\n⚠️ Coût par criticité :")
display(cost_by_criticality)

# Graphique des coûts par type d'équipement
cost_data = cost_by_equipment_type.toPandas()
plt.figure(figsize=(12, 6))
plt.bar(cost_data['equipment_type'], cost_data['cout_total'], color='lightcoral')
plt.title('Coût total de maintenance par type d\'\u00e9quipement', fontsize=14, fontweight='bold')
plt.xlabel('Type d\'\u00e9quipement')
plt.ylabel('Coût total (€)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,👥 KPI 4 : Performance des techniciens
# Analyse de la performance des techniciens
print("👥 KPI 4 : PERFORMANCE DES TECHNICIENS")
print("=" * 45)

# Performance globale par technicien
technician_performance = spark.sql("""
    SELECT 
        assigned_technician,
        COUNT(DISTINCT work_order_id) as nombre_ordres_traites,
        ROUND(AVG(CASE WHEN is_delayed = false THEN 1.0 ELSE 0.0 END) * 100, 2) as taux_respect_delais,
        ROUND(AVG(CAST(duration_hours AS DOUBLE)), 2) as duree_moyenne_intervention,
        ROUND(SUM(total_cost), 2) as cout_total_interventions
    FROM maintenance_unified 
    WHERE status = 'Terminé' AND assigned_technician IS NOT NULL
    GROUP BY assigned_technician
    ORDER BY nombre_ordres_traites DESC
""")

print("🎯 Performance par technicien :")
display(technician_performance)

# Efficacité par technicien (coût par heure)
technician_efficiency = spark.sql("""
    SELECT 
        assigned_technician,
        ROUND(AVG(total_cost / CAST(duration_hours AS DOUBLE)), 2) as cout_par_heure,
        COUNT(*) as nombre_interventions
    FROM maintenance_unified 
    WHERE intervention_id IS NOT NULL 
        AND CAST(duration_hours AS DOUBLE) > 0 
        AND total_cost IS NOT NULL
        AND assigned_technician IS NOT NULL
    GROUP BY assigned_technician
    ORDER BY cout_par_heure ASC
""")

print("\n⏱️ Efficacité par technicien (coût/heure) :")
display(technician_efficiency)

# Graphique de performance
perf_data = technician_performance.toPandas()
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

# Nombre d'ordres traités
ax1.bar(perf_data['assigned_technician'], perf_data['nombre_ordres_traites'], color='skyblue')
ax1.set_title('Nombre d\'ordres traités par technicien')
ax1.set_xlabel('Technicien')
ax1.set_ylabel('Nombre d\'ordres')
ax1.tick_params(axis='x', rotation=45)

# Taux de respect des délais
ax2.bar(perf_data['assigned_technician'], perf_data['taux_respect_delais'], color='lightgreen')
ax2.set_title('Taux de respect des délais par technicien')
ax2.set_xlabel('Technicien')
ax2.set_ylabel('Taux de respect (%)')
ax2.set_ylim(0, 100)
ax2.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,📉 KPI 5 : Analyse de la maintenance préventive vs corrective
# Analyse maintenance préventive vs corrective
print("📉 KPI 5 : MAINTENANCE PRÉVENTIVE VS CORRECTIVE")
print("=" * 55)

# Répartition préventive vs corrective
maintenance_type_analysis = spark.sql("""
    SELECT 
        work_order_type,
        COUNT(*) as nombre_ordres,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pourcentage,
        ROUND(AVG(total_cost), 2) as cout_moyen,
        ROUND(AVG(duration_hours), 2) as duree_moyenne
    FROM maintenance_unified 
    WHERE work_order_type IN ('Maintenance préventive', 'Maintenance corrective')
        AND intervention_id IS NOT NULL
    GROUP BY work_order_type
    ORDER BY nombre_ordres DESC
""")

print("🎯 Répartition maintenance préventive vs corrective :")
display(maintenance_type_analysis)

# Impact de la maintenance préventive sur les pannes
preventive_impact = spark.sql("""
    SELECT 
        equipment_id,
        equipment_name,
        SUM(CASE WHEN work_order_type = 'Maintenance préventive' THEN 1 ELSE 0 END) as nb_preventive,
        SUM(CASE WHEN work_order_type = 'Maintenance corrective' THEN 1 ELSE 0 END) as nb_corrective,
        ROUND(
            SUM(CASE WHEN work_order_type = 'Maintenance préventive' THEN 1 ELSE 0 END) * 1.0 /
            NULLIF(SUM(CASE WHEN work_order_type = 'Maintenance corrective' THEN 1 ELSE 0 END), 0), 2
        ) as ratio_preventive_corrective
    FROM maintenance_unified 
    WHERE work_order_type IN ('Maintenance préventive', 'Maintenance corrective')
    GROUP BY equipment_id, equipment_name
    HAVING SUM(CASE WHEN work_order_type = 'Maintenance corrective' THEN 1 ELSE 0 END) > 0
    ORDER BY ratio_preventive_corrective DESC
""")

print("\n🔧 Impact de la maintenance préventive (Top 10) :")
display(preventive_impact.limit(10))

# Graphique en secteurs
maint_data = maintenance_type_analysis.toPandas()
plt.figure(figsize=(10, 8))
plt.pie(maint_data['nombre_ordres'], labels=maint_data['work_order_type'], autopct='%1.1f%%', startangle=90)
plt.title('Répartition Maintenance Préventive vs Corrective', fontsize=14, fontweight='bold')
plt.axis('equal')
plt.show()

# COMMAND ----------

# DBTITLE 1,📊 Dashboard KPI - Résumé exécutif
# Création d'un dashboard résumé avec tous les KPI principaux
print("📊 DASHBOARD KPI - RÉSUMÉ EXÉCUTIF")
print("=" * 50)

# Collecte de tous les KPI principaux
executive_summary = spark.sql("""
    WITH kpi_data AS (
        SELECT 
            COUNT(DISTINCT equipment_id) as total_equipements,
            COUNT(DISTINCT work_order_id) as total_ordres_travail,
            COUNT(DISTINCT intervention_id) as total_interventions,
            
            -- Taux de respect des délais
            ROUND(
                SUM(CASE WHEN is_delayed = false AND status = 'Terminé' THEN 1 ELSE 0 END) * 100.0 / 
                NULLIF(SUM(CASE WHEN status = 'Terminé' THEN 1 ELSE 0 END), 0), 2
            ) as taux_respect_delais,
            
            -- MTTR
            ROUND(AVG(duration_hours), 2) as mttr_heures,
            
            -- Coûts
            ROUND(SUM(total_cost), 2) as cout_total_maintenance,
            ROUND(AVG(total_cost), 2) as cout_moyen_intervention,
            
            -- Maintenance préventive vs corrective
            ROUND(
                SUM(CASE WHEN work_order_type = 'Maintenance préventive' THEN 1 ELSE 0 END) * 100.0 /
                NULLIF(SUM(CASE WHEN work_order_type IN ('Maintenance préventive', 'Maintenance corrective') THEN 1 ELSE 0 END), 0), 2
            ) as taux_maintenance_preventive
            
        FROM maintenance_unified
    )
    SELECT * FROM kpi_data
""")

print("🎯 INDICATEURS CLÉS DE PERFORMANCE :")
print("=" * 40)
display(executive_summary)

# Création d'un résumé visuel
summary_data = executive_summary.collect()[0]

# Graphique de synthèse
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

# KPI 1: Taux de respect des délais
ax1.pie([summary_data['taux_respect_delais'], 100-summary_data['taux_respect_delais']], 
        labels=['Dans les temps', 'En retard'], 
        colors=['green', 'red'], autopct='%1.1f%%')
ax1.set_title('Taux de respect des délais')

# KPI 2: Répartition des coûts (exemple)
ax2.bar(['Coût total'], [summary_data['cout_total_maintenance']], color='orange')
ax2.set_title('Coût total de maintenance')
ax2.set_ylabel('Coût (€)')

# KPI 3: MTTR
ax3.bar(['MTTR'], [summary_data['mttr_heures']], color='blue')
ax3.set_title('MTTR (Mean Time To Repair)')
ax3.set_ylabel('Heures')

# KPI 4: Maintenance préventive vs corrective
ax4.pie([summary_data['taux_maintenance_preventive'], 100-summary_data['taux_maintenance_preventive']], 
        labels=['Préventive', 'Corrective'], 
        colors=['lightblue', 'lightcoral'], autopct='%1.1f%%')
ax4.set_title('Répartition Préventive/Corrective')

plt.suptitle('Dashboard KPI - Maintenance GMAO', fontsize=16, fontweight='bold')
plt.tight_layout()
plt.show()

print("\n✅ Dashboard KPI créé avec succès !")
print("🚀 Analyse complète de la maintenance terminée !")

# COMMAND ----------

# DBTITLE 1,🎯 Query 1: KPI Summary Card
# MAGIC %md
# MAGIC ---
# MAGIC # 🎉 Conclusion de l'atelier
# MAGIC
# MAGIC ## 🎯 Ce que vous avez appris
# MAGIC
# MAGIC Félicitations ! Vous avez réalisé votre premier projet complet sur Databricks. Voici ce que vous maîtrisez maintenant :
# MAGIC
# MAGIC ### 📥 **Ingestion de données**
# MAGIC * Utilisation d'**Autoloader** pour charger automatiquement des fichiers CSV
# MAGIC * Création de tables **Delta Lake** pour un stockage optimisé
# MAGIC * Gestion des schémas et des checkpoints
# MAGIC
# MAGIC ### 🔍 **Exploration et analyse**
# MAGIC * Exploration interactive des données avec **PySpark**
# MAGIC * Création de visualisations avec **Matplotlib**
# MAGIC * Analyse statistique des données de maintenance
# MAGIC
# MAGIC ### 🔧 **Transformation de données**
# MAGIC * Nettoyage et enrichissement des données
# MAGIC * Jointures complexes entre plusieurs tables
# MAGIC * Création de colonnes calculées et de catégories
# MAGIC
# MAGIC ### 📊 **Calcul d'indicateurs métier**
# MAGIC * **KPI de maintenance** : MTTR, taux de respect des délais, coûts
# MAGIC * **Analyse de performance** des techniciens
# MAGIC * **Dashboards visuels** pour le pilotage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Prochaines étapes
# MAGIC
# MAGIC ### Pour aller plus loin avec Databricks :
# MAGIC
# MAGIC * **🤖 Machine Learning** : Prédire les pannes avec MLflow
# MAGIC * **📈 Dashboards avancés** : Créer des tableaux de bord interactifs
# MAGIC * **⏰ Automatisation** : Planifier des jobs de traitement
# MAGIC * **🔄 Streaming** : Traiter des données en temps réel
# MAGIC * **📊 SQL Analytics** : Requêtes SQL avancées sur vos données
# MAGIC
# MAGIC ### Ressources utiles :
# MAGIC * [Documentation Databricks](https://docs.databricks.com/)
# MAGIC * [Databricks Academy](https://academy.databricks.com/)
# MAGIC * [Communauté Databricks](https://community.databricks.com/)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Questions et discussion
# MAGIC
# MAGIC **Avez-vous des questions sur :**
# MAGIC * Les concepts abordés dans cet atelier ?
# MAGIC * L'application à vos cas d'usage spécifiques ?
# MAGIC * Les fonctionnalités avancées de Databricks ?
# MAGIC
# MAGIC **Merci d'avoir participé à cet atelier Databricks - GMAO !** 🎉
