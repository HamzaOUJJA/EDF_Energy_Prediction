# EDF Energy Prediction — MSPR TPRE932 / TPRE942

Plateforme de **data engineering**, **machine learning** et **monitoring** pour la prédiction de la consommation électrique nationale à partir des données **RTE éCO2mix**.

Ce projet a été réalisé dans le cadre de la MSPR 2025–2026 pour la certification **Chef de Projet Expert en Intelligence Artificielle — RNCP36582 Niveau 7**.

L’objectif n’est pas seulement de construire un modèle de prédiction, mais de préparer une solution IA **déployable, maintenable, supervisable et exploitable** dans un environnement technique complet.

---

## 1. Objectifs du projet

Le projet **EDF Energy Prediction** vise à mettre en place une chaîne complète permettant de :

* ingérer les données RTE éCO2mix ;
* organiser les données selon une architecture **Bronze → Silver → Gold** ;
* nettoyer, contrôler et enrichir les données ;
* entraîner plusieurs modèles de machine learning ;
* comparer les performances des modèles ;
* sélectionner le meilleur modèle ;
* stocker les métriques et les artefacts ML ;
* générer des rapports EDA et ML ;
* superviser les résultats avec Grafana ;
* préparer la maintenabilité, le monitoring, le rollback et le déploiement de la solution IA.

---

## 2. Contexte métier

La consommation électrique nationale varie selon plusieurs facteurs : saison, heure, jour de la semaine, usages, historique de consommation et prévisions disponibles.

Dans un contexte énergétique, la capacité à anticiper la consommation permet de mieux suivre l’équilibre entre production et demande. La prédiction de consommation est donc un cas d’usage pertinent pour une solution d’intelligence artificielle appliquée au secteur de l’énergie.

Le projet utilise les données **RTE éCO2mix**, qui fournissent des informations sur la consommation électrique, les prévisions J-1, le mix énergétique et certains indicateurs liés au système électrique français.

---

## 3. Architecture générale

La solution repose sur une architecture de type **Data Lake + MLOps simplifié**.

```text
RTE éCO2mix
    ↓
Bronze
    ↓
Silver
    ↓
Gold
    ↓
Machine Learning
    ↓
PostgreSQL / MinIO
    ↓
Grafana / Rapports EDA-ML
```

### Rôle des principales couches

| Couche     | Rôle                                     | Objectif                                          |
| ---------- | ---------------------------------------- | ------------------------------------------------- |
| Bronze     | Conservation des données brutes          | Garder une trace fidèle de la source              |
| Silver     | Nettoyage, harmonisation, enrichissement | Produire des données fiables                      |
| Gold       | Agrégations analytiques                  | Préparer les données pour reporting et ML         |
| Modeling   | Entraînement et comparaison des modèles  | Sélectionner le meilleur modèle                   |
| Reporting  | Génération des visuels EDA / ML          | Vérifier les résultats et produire des preuves    |
| Monitoring | Suivi avec Grafana                       | Superviser la qualité, les métriques et le modèle |

---

## 4. Stack technique

| Composant      | Rôle dans le projet                          | Port local  |
| -------------- | -------------------------------------------- | ----------- |
| Docker Compose | Conteneurisation de la plateforme            | —           |
| Apache Airflow | Orchestration des pipelines                  | 8081        |
| Apache Spark   | Traitements distribués et ML                 | 7077 / 8082 |
| MinIO          | Stockage objet compatible S3                 | 9000 / 9001 |
| PostgreSQL     | Stockage des métriques et tables analytiques | 5432        |
| Grafana        | Dashboards de monitoring                     | 3001        |
| Prometheus     | Collecte de métriques techniques             | 9090        |
| Kafka          | Préparation à l’ingestion événementielle     | 29092       |
| Kafka UI       | Interface de suivi Kafka                     | 8085        |

---

## 5. Pipeline principal

Le pipeline principal est orchestré par Airflow via le DAG :

```text
edf_pipeline_complet
```

Il suit la logique suivante :

```text
start
  ↓
prerequisites
  ↓
bronze
  ↓
silver
  ↓
gold
  ↓
quality
  ↓
ml
  ↓
reporting
  ↓
finalize_pipeline_run
  ↓
end
```

### Description des étapes

| Étape                 | Description                                               |
| --------------------- | --------------------------------------------------------- |
| prerequisites         | Vérifie les sources et initialise les éléments techniques |
| bronze                | Charge les données brutes RTE                             |
| silver                | Nettoie et harmonise les données                          |
| gold                  | Produit les agrégats utiles                               |
| quality               | Exécute les contrôles qualité                             |
| ml                    | Entraîne et compare les modèles                           |
| reporting             | Génère les rapports EDA et ML                             |
| finalize_pipeline_run | Finalise le run et trace son statut                       |

La partie ML contient une logique conditionnelle :

* `run_gold_to_model` : lance l’entraînement ML si les données sont prêtes ;
* `skip_ml_training` : ignore l’entraînement si les conditions ne sont pas réunies ;
* `generate_eda_ml_report` : génère les visuels ML si les artefacts sont disponibles ;
* `mark_eda_ml_pending` : marque le reporting ML comme en attente si nécessaire.

---

## 6. Modèles de machine learning

Le problème traité est un problème de **régression supervisée**.

La variable cible est :

```text
consumption_mw
```

Elle représente la consommation électrique à prédire, exprimée en MW.

### Modèles comparés

Quatre modèles ont été testés :

| Modèle            | Rôle                                            |
| ----------------- | ----------------------------------------------- |
| Linear Regression | Modèle de référence simple                      |
| Decision Tree     | Modèle interprétable à base de règles           |
| Random Forest     | Modèle robuste basé sur plusieurs arbres        |
| Gradient Boosting | Modèle ensembliste performant mais plus coûteux |

---

## 7. Prévention de la fuite de données

Une correction importante a été réalisée afin d’éviter un risque de **fuite de données** dans la partie machine learning.

Certaines variables descriptives du mix énergétique peuvent être utiles pour l’analyse et les dashboards, mais elles ne doivent pas être utilisées comme variables d’entrée du modèle si elles représentent des informations connues uniquement au moment cible.

### Variables conservées pour l’analyse descriptive

Les variables liées au mix énergétique sont conservées dans les couches Silver / Gold et dans Grafana :

* nucléaire ;
* hydraulique ;
* éolien ;
* solaire ;
* gaz ;
* charbon ;
* bioénergie ;
* taux de CO₂ ;
* parts du mix énergétique.

Elles servent à l’analyse métier et au monitoring.

### Variables utilisées pour le modèle final

Le modèle final utilise uniquement des variables disponibles avant la prédiction :

* variables temporelles ;
* encodage cyclique de l’heure ;
* indicateurs calendrier ;
* historiques de consommation ;
* moyennes glissantes ;
* prévision RTE J-1.

Exemples de variables :

```text
hour
day_of_week
day_of_year
week_of_year
month
quarter
season
is_weekend
is_peak_hour
hour_sin
hour_cos
lag_1h_mw
lag_24h_mw
lag_168h_mw
rolling_24h_mean
rolling_24h_std
forecast_j1_mw
```

Cette correction permet d’obtenir une évaluation plus réaliste du modèle.

---

## 8. Résultats du dernier run validé

Le dernier run officiel validé est :

```text
run_id = 20260609_161434
```

Le modèle retenu est :

```text
RandomForest / Forêt aléatoire
```

### Métriques finales

| Modèle           |      RMSE |       MAE |   MAPE |     R² |
| ---------------- | --------: | --------: | -----: | -----: |
| RandomForest     | 711.32 MW | 537.41 MW | 1.07 % | 0.9949 |
| GradientBoosting | 712.84 MW | 547.22 MW | 1.10 % | 0.9949 |
| LinearRegression | 741.73 MW | 582.10 MW | 1.16 % | 0.9945 |
| DecisionTree     | 758.87 MW | 582.24 MW | 1.16 % | 0.9942 |

Le modèle **RandomForest** est retenu car il obtient le RMSE le plus faible sur le jeu de test temporel.

Même si Gradient Boosting est très proche, RandomForest présente un meilleur compromis entre précision, robustesse et simplicité de déploiement.

---

## 9. Interprétation des résultats

Les résultats montrent que le modèle suit correctement la dynamique de la consommation électrique.

Le **MAPE de 1.07 %** indique que l’erreur moyenne relative reste faible. Cela signifie que le modèle produit des prédictions cohérentes par rapport aux niveaux de consommation observés.

Le **R² de 0.9949** montre que le modèle explique une grande partie de la variabilité de la consommation.

Les visualisations générées permettent également de vérifier :

* la comparaison réel vs prédit ;
* la stabilité temporelle des prédictions ;
* les erreurs résiduelles ;
* la comparaison entre modèles ;
* la cohérence du split temporel 80/20.

---

## 10. Rapports EDA et ML

Le projet génère automatiquement des graphiques utilisés pour le rapport professionnel.

### Exemples de rapports EDA

* impact du nettoyage sur la consommation nationale ;
* volume mensuel avant / après nettoyage ;
* diagnostic de qualité des données ;
* répartition train/test temporelle.

### Exemples de rapports ML

* comparaison des modèles ;
* synthèse des performances ML ;
* prédictions réel vs prédit ;
* série temporelle des prédictions ;
* courbe d’apprentissage.

Les images sont générées dans :

```text
/opt/airflow/data/eda/report
```

et peuvent être copiées localement avec :

```bash
docker cp edf-airflow-webserver:/opt/airflow/data/eda/report/. ./eda_report_images_clean
```

---

## 11. Monitoring Grafana

Grafana permet de suivre les indicateurs clés de la solution.

Les dashboards permettent notamment de visualiser :

* la consommation réelle vs la prévision J-1 ;
* les agrégats journaliers ;
* le mix énergétique ;
* le meilleur modèle ;
* le RMSE minimal ;
* le R² maximal ;
* les métriques détaillées par modèle ;
* le nombre de runs ML enregistrés.

Le dernier dashboard ML confirme :

```text
Meilleur modèle : RandomForest
RMSE minimal : 711.3
R² max : 99.490 %
Runs ML enregistrés : 4
```

---

## 12. Lancement de la plateforme

Depuis la racine du projet :

```bash
docker compose up -d
```

Vérifier les services :

```bash
docker compose ps
```

Arrêter les services sans supprimer les volumes :

```bash
docker compose down
```

Attention : la commande suivante supprime les volumes Docker et doit être utilisée avec prudence :

```bash
docker compose down -v
```

---

## 13. Interfaces principales

| Service      | URL                   |
| ------------ | --------------------- |
| Webapp       | http://localhost:5000 |
| Airflow      | http://localhost:8081 |
| Spark Master | http://localhost:8082 |
| MinIO        | http://localhost:9001 |
| Grafana      | http://localhost:3001 |
| Prometheus   | http://localhost:9090 |
| Kafka UI     | http://localhost:8085 |

---

## 14. Vérifications utiles

### Vérifier la résolution DNS Docker

```bash
docker compose exec airflow-webserver getent hosts postgres
docker compose exec airflow-webserver getent hosts spark-master
```

### Vérifier PostgreSQL

```bash
docker compose exec postgres pg_isready -U edf -d edf_dw
```

### Vérifier les dernières métriques ML

```bash
docker compose exec postgres psql -U edf -d edf_dw -P pager=off -c "SELECT run_id, model_name, ROUND(rmse::numeric,2) AS rmse, ROUND(mae::numeric,2) AS mae, ROUND(mape_pct::numeric,2) AS mape, ROUND(r2::numeric,4) AS r2, trained_at FROM etl.model_metrics ORDER BY trained_at DESC, rmse ASC LIMIT 8;"
```

### Vérifier les variables ML utilisées

```bash
docker compose exec airflow-webserver python -c "from spark.ml.constants import CANDIDATE_FEATURES, FORBIDDEN_LEAKAGE_FEATURES; print('CANDIDATE_FEATURES =', CANDIDATE_FEATURES); print('Nombre features =', len(CANDIDATE_FEATURES)); print('Leakage =', set(CANDIDATE_FEATURES).intersection(FORBIDDEN_LEAKAGE_FEATURES))"
```

Le résultat attendu est :

```text
Leakage = set()
```

---

## 15. Gestion des artefacts ML

Les artefacts ML sont stockés dans MinIO et synchronisés localement.

Les artefacts de reporting ML incluent notamment :

```text
predictions.parquet
learning_curve.parquet
split_summary.json
run_meta.parquet
```

Un point important a été corrigé : certains anciens artefacts locaux pouvaient rester présents et générer des graphiques obsolètes. La solution consiste à supprimer les anciens artefacts locaux puis à resynchroniser les artefacts du dernier run validé.

Exemple de vérification :

```bash
docker compose exec airflow-webserver bash -lc 'ls -lh --full-time /opt/airflow/data/models/rte/_report'
```

Calcul direct des métriques depuis `predictions.parquet` :

```bash
python - <<'PY'
import pandas as pd
import numpy as np

path = "/opt/airflow/data/models/rte/_report/predictions.parquet"
df = pd.read_parquet(path)

actual = df["actual_mw"].astype(float)
pred = df["predicted_mw"].astype(float)
err = actual - pred

rmse = np.sqrt((err ** 2).mean())
mae = np.abs(err).mean()
mape = (np.abs(err) / actual.replace(0, np.nan)).mean() * 100

print("RMSE:", round(rmse, 2))
print("MAE :", round(mae, 2))
print("MAPE:", round(mape, 2))
PY
```

Résultat attendu pour le dernier run propre :

```text
RMSE : 711.32
MAE  : 537.41
MAPE : 1.07
```

---

## 16. Maintenabilité

La solution intègre plusieurs éléments de maintenabilité :

* orchestration claire du pipeline avec Airflow ;
* séparation des couches Bronze, Silver et Gold ;
* logs Airflow et Spark ;
* métriques stockées dans PostgreSQL ;
* artefacts ML stockés dans MinIO ;
* visualisation des résultats avec Grafana ;
* contrôle de la cohérence des métriques ;
* possibilité de relancer une tâche ciblée ;
* possibilité de comparer plusieurs modèles ;
* possibilité de revenir à une version stable du modèle.

---

## 17. Runbook rapide

### Démarrer la plateforme

```bash
docker compose up -d
```

### Vérifier les conteneurs

```bash
docker compose ps
```

### Lancer le pipeline depuis Airflow

Ouvrir :

```text
http://localhost:8081
```

Puis déclencher le DAG :

```text
edf_pipeline_complet
```

### Consulter Spark

```text
http://localhost:8082
```

### Consulter MinIO

```text
http://localhost:9001
```

### Consulter Grafana

```text
http://localhost:3001
```

### Consulter les logs Airflow

```bash
docker compose logs airflow-webserver
docker compose logs airflow-scheduler
```

### Consulter les logs Spark

```bash
docker compose logs spark-master
docker compose logs spark-worker-1
docker compose logs spark-worker-2
```

---

## 18. Incidents fréquents

| Incident                        | Cause possible                        | Action corrective                         |
| ------------------------------- | ------------------------------------- | ----------------------------------------- |
| Airflow affiche une tâche rouge | Erreur script ou service indisponible | Lire les logs de la tâche                 |
| Spark driver en erreur          | Ressources, script ou réseau Docker   | Vérifier Spark UI et logs workers         |
| DNS Docker instable             | Problème réseau Docker temporaire     | Redémarrer les services ou Docker Desktop |
| Grafana vide                    | Source de données absente             | Vérifier PostgreSQL / Prometheus          |
| Images ML incohérentes          | Ancien artefact local                 | Supprimer `_report` et resynchroniser     |
| Modèle absent                   | Entraînement non terminé              | Relancer `run_gold_to_model`              |
| MinIO vide                      | Étape précédente non exécutée         | Relancer Bronze / Silver / Gold           |

---

## 19. Sécurité et RGPD

Les données RTE éCO2mix utilisées dans ce projet sont des données énergétiques agrégées. Elles ne contiennent pas directement de données personnelles.

Cependant, le projet prend en compte les principes suivants :

* limitation des données utilisées ;
* séparation des rôles techniques ;
* protection des accès aux interfaces ;
* protection des secrets et variables d’environnement ;
* contrôle des accès aux buckets MinIO ;
* traçabilité des traitements ;
* conservation des logs d’exécution ;
* supervision des services critiques.

L’analyse DIC est également prise en compte :

| Axe             | Risque                                            | Mesure                                                    |
| --------------- | ------------------------------------------------- | --------------------------------------------------------- |
| Disponibilité   | Service Airflow / Spark / PostgreSQL indisponible | Monitoring, logs, redémarrage contrôlé                    |
| Intégrité       | Données ou artefacts obsolètes                    | Contrôles qualité, versioning, vérification des métriques |
| Confidentialité | Accès non autorisé aux interfaces                 | Authentification, gestion des droits, secrets hors code   |

---

## 20. Limites actuelles

La solution actuelle fonctionne dans un environnement de simulation Docker Compose. Elle n’est pas encore une production réelle.

Limites identifiées :

* haute disponibilité non complète ;
* sauvegardes à automatiser ;
* alertes Grafana à connecter à email ou Teams ;
* tests de charge à renforcer ;
* suivi de dérive modèle à améliorer ;
* variables météo non encore intégrées ;
* exposition API du modèle non encore finalisée ;
* versioning MLOps à renforcer avec un outil comme MLflow.

---

## 21. Perspectives d’amélioration

Les prochaines évolutions possibles sont :

* déploiement sur Kubernetes ;
* intégration d’un outil MLOps comme MLflow ;
* ajout d’une API de prédiction ;
* ajout de données météo ;
* ajout des jours fériés et vacances scolaires ;
* alertes Grafana automatiques ;
* sauvegarde automatique PostgreSQL / MinIO ;
* tests de charge ;
* monitoring avancé de la dérive des données et du modèle.

---

## 22. Structure simplifiée du projet

```text
edf-energy-prediction/
├── dags/
├── plugins/
├── spark/
│   ├── jobs/
│   ├── transform/
│   ├── ml/
│   └── common/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── models/
│   └── eda/
├── docker-compose.yml
├── Makefile
├── README.md
└── .gitignore
```

---

## 23. Auteurs

Projet réalisé dans le cadre de la MSPR EPSI 2025–2026.

Membres du groupe :

* Mohamed Barhami
*christian
hamza
benoit
youssef

---

## 24. Licence

Projet académique réalisé dans le cadre de la formation EPSI.
Les données utilisées proviennent de RTE éCO2mix.
