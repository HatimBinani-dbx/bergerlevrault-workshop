# Databricks notebook source
# MAGIC %md
# MAGIC # 🏭 Atelier Databricks - GMAO avec Berger Levrault
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

# DBTITLE 1,📁 Création du répertoire de données
# Création d'un répertoire pour stocker nos fichiers CSV
import os
from datetime import datetime, timedelta
import random

# Définition du chemin de travail
workshop_path = "/Workspace/Users/" + spark.sql("SELECT current_user()").collect()[0][0] + "/bergerlevrault-workshop"
data_path = workshop_path + "/data"

# Création des répertoires si ils n'existent pas
dbutils.fs.mkdirs(data_path)

print(f"📁 Répertoire des données : {data_path}")
print("\n✅ Structure des dossiers prête pour l'atelier !")

# COMMAND ----------

# DBTITLE 1,🏭 Génération des données GMAO - Équipements
# Génération des données d'équipements
import csv
import random
from datetime import datetime, timedelta

# Utilitaire: écriture de CSV en plusieurs fichiers (chunking)
def write_csv_chunks(records, directory_path, base_filename, chunk_size):
    if not records:
        return
    for start_idx in range(0, len(records), chunk_size):
        chunk = records[start_idx:start_idx + chunk_size]
        part_number = (start_idx // chunk_size) + 1
        output_file = f"{directory_path}/{base_filename}_{part_number}.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=chunk[0].keys())
            writer.writeheader()
            writer.writerows(chunk)
        print(f"📄 Fichier sauvegardé : {output_file} ({len(chunk)} lignes)")

# Définition des types d'équipements typiques en industrie
equipment_types = [
    "Compresseur", "Pompe centrifuge", "Moteur électrique", "Convoyeur", 
    "Ventilateur", "Réducteur", "Transformateur", "Groupe électrogène",
    "Chaudière", "Échangeur thermique", "Presse hydraulique", "Robot industriel"
]

locations = [
    "Atelier A", "Atelier B", "Atelier C", "Zone de stockage", 
    "Salle des machines", "Chaufferie", "Station de pompage", "Ligne de production 1",
    "Ligne de production 2", "Zone de conditionnement"
]

manufacturers = ["Siemens", "ABB", "Schneider", "Danfoss", "Grundfos", "Atlas Copco", "Bosch Rexroth"]

# Paramètres de génération
EQUIPMENTS_COUNT = 500
EQUIPMENTS_CHUNK_SIZE = 100

# Génération d'un grand nombre d'équipements
equipments_data = []
for i in range(1, EQUIPMENTS_COUNT + 1):
    install_date = datetime.now() - timedelta(days=random.randint(365, 3650))  # Entre 1 et 10 ans
    
    equipment = {
        'equipment_id': f'EQ{i:03d}',
        'equipment_name': f'{random.choice(equipment_types)} {i:02d}',
        'equipment_type': random.choice(equipment_types),
        'location': random.choice(locations),
        'manufacturer': random.choice(manufacturers),
        'model': f'MOD-{random.randint(1000, 9999)}',
        'serial_number': f'SN{random.randint(100000, 999999)}',
        'installation_date': install_date.strftime('%Y-%m-%d'),
        'criticality': random.choice(['Critique', 'Important', 'Standard']),
        'status': random.choice(['En service', 'En service', 'En service', 'En maintenance', 'Arrêté'])
    }
    equipments_data.append(equipment)

# Sauvegarde en plusieurs CSV (chunking)
print(f"✅ Données équipements générées : {len(equipments_data)} enregistrements")
write_csv_chunks(equipments_data, data_path, "equipments", EQUIPMENTS_CHUNK_SIZE)
print("\n📋 Aperçu des données :")
for i, eq in enumerate(equipments_data[:3]):
    print(f"  {i+1}. {eq['equipment_name']} - {eq['location']} - {eq['criticality']}")

# COMMAND ----------

# DBTITLE 1,📋 Génération des données GMAO - Ordres de travail
# Génération des ordres de travail de maintenance

work_order_types = [
    "Maintenance préventive", "Maintenance corrective", "Inspection", 
    "Révision générale", "Dépannage urgent", "Amélioration", "Nettoyage"
]

priorities = ["Très urgent", "Urgent", "Normal", "Programmé"]
statuses = ["Ouvert", "En cours", "Terminé", "Annulé"]
technicians = ["Martin Dubois", "Sophie Laurent", "Pierre Moreau", "Marie Durand", "Jean Leroy", "Claire Bernard"]

# Paramètres de génération
WORK_ORDERS_COUNT = 5000
WORK_ORDERS_CHUNK_SIZE = 1000

# Génération d'un grand nombre d'ordres de travail
work_orders_data = []
for i in range(1, WORK_ORDERS_COUNT + 1):
    # Date de création dans les 6 derniers mois
    creation_date = datetime.now() - timedelta(days=random.randint(1, 180))
    
    # Sélection d'un équipement aléatoire
    equipment = random.choice(equipments_data)
    
    # Statut et dates en fonction de l'ancienneté
    if creation_date < datetime.now() - timedelta(days=30):
        status = random.choice(["Terminé", "Terminé", "Terminé", "Annulé"])
    elif creation_date < datetime.now() - timedelta(days=7):
        status = random.choice(["En cours", "Terminé", "Terminé"])
    else:
        status = random.choice(["Ouvert", "En cours"])
    
    # Dates de planification et de fin
    planned_date = creation_date + timedelta(days=random.randint(1, 14))
    completion_date = None
    if status == "Terminé":
        completion_date = planned_date + timedelta(days=random.randint(-2, 5))
    
    work_order = {
        'work_order_id': f'WO{i:04d}',
        'equipment_id': equipment['equipment_id'],
        'work_order_type': random.choice(work_order_types),
        'description': f'Maintenance {equipment["equipment_name"]} - {random.choice(["Contrôle", "Réparation", "Remplacement", "Vérification"])}',
        'priority': random.choice(priorities),
        'status': status,
        'creation_date': creation_date.strftime('%Y-%m-%d'),
        'planned_date': planned_date.strftime('%Y-%m-%d'),
        'completion_date': completion_date.strftime('%Y-%m-%d') if completion_date else None,
        'assigned_technician': random.choice(technicians),
        'estimated_hours': random.randint(1, 16),
        'actual_hours': random.randint(1, 20) if status == "Terminé" else None
    }
    work_orders_data.append(work_order)

# Sauvegarde en plusieurs CSV (chunking)
print(f"✅ Données ordres de travail générées : {len(work_orders_data)} enregistrements")
write_csv_chunks(work_orders_data, data_path, "work_orders", WORK_ORDERS_CHUNK_SIZE)
print("\n📋 Aperçu des données :")
for i, wo in enumerate(work_orders_data[:3]):
    print(f"  {i+1}. {wo['work_order_id']} - {wo['work_order_type']} - {wo['status']}")

# COMMAND ----------

# DBTITLE 1,🔧 Génération des données GMAO - Interventions
# Génération des données d'interventions détaillées
import builtins

intervention_types = [
    "Diagnostic", "Réparation", "Remplacement pièce", "Graissage", 
    "Nettoyage", "Contrôle visuel", "Mesure vibrations", "Test fonctionnel",
    "Étalonnage", "Serrage boulonnerie", "Changement filtre", "Vidange"
]

parts_used = [
    "Roulement", "Courroie", "Joint", "Filtre à huile", "Filtre à air", 
    "Huile hydraulique", "Graisse", "Contacteur", "Fusible", "Capteur",
    "Vérin", "Flexible", "Boulon", "Écrou", "Ressort"
]

# Paramètres de génération
INTERVENTIONS_CHUNK_SIZE = 2000

# Génération d'interventions pour les ordres de travail terminés
interventions_data = []
intervention_id = 1

for work_order in work_orders_data:
    if work_order['status'] == 'Terminé':
        # Nombre d'interventions par ordre (1 à 3)
        num_interventions = random.randint(1, 3)
        
        for i in range(num_interventions):
            intervention_date = datetime.strptime(work_order['completion_date'], '%Y-%m-%d')
            if i > 0:
                intervention_date = intervention_date - timedelta(days=random.randint(0, 2))
            
            # Coût des pièces (peut être 0 pour certaines interventions)
            parts_cost = 0
            parts_description = ""
            if random.random() > 0.4:
                parts_cost = random.randint(10, 500)
                parts_description = random.choice(parts_used)
            
            intervention = {
                'intervention_id': f'INT{intervention_id:04d}',
                'work_order_id': work_order['work_order_id'],
                'intervention_date': intervention_date.strftime('%Y-%m-%d'),
                'technician': work_order['assigned_technician'],
                'intervention_type': random.choice(intervention_types),
                'duration_hours': builtins.round(random.uniform(0.5, 8.0), 1),
                'parts_used': parts_description,
                'parts_cost': parts_cost,
                'labor_cost': builtins.round(random.uniform(50, 400), 2),
                'comments': f'Intervention {random.choice(["réussie", "complète", "conforme", "satisfaisante"])}'
            }
            interventions_data.append(intervention)
            intervention_id += 1

# Sauvegarde en plusieurs CSV (chunking)
print(f"✅ Données interventions générées : {len(interventions_data)} enregistrements")
write_csv_chunks(interventions_data, data_path, "interventions", INTERVENTIONS_CHUNK_SIZE)
print("\n📋 Aperçu des données :")
for i, inter in enumerate(interventions_data[:3]):
    print(f"  {i+1}. {inter['intervention_id']} - {inter['intervention_type']} - {inter['duration_hours']}h")

# COMMAND ----------

# DBTITLE 1,Génération terminée
print("\n🎉 Génération des données GMAO terminée !")
print(f"📊 Résumé : {len(equipments_data)} équipements, {len(work_orders_data)} ordres de travail, {len(interventions_data)} interventions")

# COMMAND ----------

# DBTITLE 1,📂 Vérification des fichiers générés
# Vérification que tous les fichiers ont été créés correctement
print("📂 Contenu du répertoire de données :")
files = dbutils.fs.ls(data_path)
for file in files:
    print(f"  📄 {file.name} ({file.size} bytes)")

print("\n✅ Tous les fichiers CSV sont prêts pour l'ingestion avec Autoloader !")
print("\n🚀 Passons maintenant à l'étape d'ingestion des données...")

# COMMAND ----------

# DBTITLE 1,Creation du catalog/schema/volume
# Création du catalog, du schema et du volume Unity Catalog
spark.sql("CREATE CATALOG IF NOT EXISTS gmao_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS gmao_catalog.gmao_schema")
spark.sql("""
    CREATE VOLUME IF NOT EXISTS gmao_catalog.gmao_schema.gmao_volume
    COMMENT 'Volume pour les données GMAO'
""")

# Création des sous-dossiers dans le volume
dbutils.fs.mkdirs("/Volumes/gmao_catalog/gmao_schema/gmao_volume/equipments")
dbutils.fs.mkdirs("/Volumes/gmao_catalog/gmao_schema/gmao_volume/work_orders")
dbutils.fs.mkdirs("/Volumes/gmao_catalog/gmao_schema/gmao_volume/interventions")

# Copie des fichiers générés (plusieurs fichiers par dataset) vers le volume Unity Catalog
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

# COMMAND ----------

