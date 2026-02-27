# Script de la vidéo de démonstration

**Durée totale : 10 minutes max**

---

## 0:00 – 0:30 | Introduction (30s)

> "Bonjour, nous sommes Grégoire Petit et Benjamin Lepourtois, étudiants du Mastère Spécialisé Big Data à Télécom Paris. Nous vous présentons notre projet : un pipeline big data de bout en bout pour l'analyse et la prédiction de matchs NBA."

**Montrer :** Page de titre du rapport PDF.

**Points clés à mentionner :**
- Pipeline end-to-end : de l'API brute au dashboard
- Stack : Spark, Airflow, MinIO, Elasticsearch, Kibana, Docker
- Objectif ML : prédire la probabilité de victoire à domicile

---

## 0:30 – 1:30 | Architecture globale (1 min)

> "Voici l'architecture de notre pipeline."

**Montrer :** Le schéma d'architecture du rapport (Figure 1) ou un slide dédié.

**Dérouler le flux :**
1. **Ingestion** — 2 APIs publiques (balldontlie pour les matchs/scores, TheSportsDB pour les salles/villes)
2. **Stockage** — Data lake MinIO, S3-compatible, organisé en 3 couches : `raw/`, `formatted/`, `combined/`
3. **Traitement** — Apache Spark : formatage JSON→Parquet, calcul de KPIs, agrégations glissantes
4. **ML** — Régression logistique PySpark pour la prédiction de victoire
5. **Indexation** — Elasticsearch (2 indices : `nba_team_metrics`, `nba_match_metrics`)
6. **Visualisation** — Dashboard Kibana avec 6 visualisations pré-configurées
7. **Orchestration** — DAG Airflow quotidien, tout s'exécute en une commande

---

## 1:30 – 2:30 | Lancement de la stack Docker (1 min)

> "L'ensemble de l'infrastructure tourne dans Docker Compose avec 8 services."

**Montrer dans le terminal :**

```bash
# Montrer le docker-compose.yml (survol rapide)
cat docker-compose.yml | head -30

# Lancer la stack (ou montrer qu'elle tourne déjà)
docker-compose ps
```

**Points clés :**
- 8 services : Airflow (webserver + scheduler), Spark (master + worker), MinIO, Elasticsearch, Kibana, PostgreSQL
- Multi-stage build pour l'image Airflow (embarque Spark + Java)
- Volumes persistants pour les données
- Tout se lance avec `docker-compose up -d`

---

## 2:30 – 4:00 | Ingestion des données (1 min 30)

> "Commençons par l'ingestion. Nous collectons des données depuis deux APIs."

**Montrer dans le terminal :**

```bash
# Montrer le script d'ingestion
head -50 ingestion/ingest_balldontlie.py

# Montrer les données brutes dans MinIO (console web ou CLI)
# Accéder à http://localhost:9001 (MinIO Console)
```

**Points clés à expliquer :**
- **balldontlie API** : ~1 300 matchs par saison, scores, équipes, conférences
- **TheSportsDB API** : noms de salles, villes, capacité
- Gestion du **rate limiting** avec exponential backoff (5 retries)
- **Pagination par curseur** automatique
- Mode **incrémental** (`--incremental`) : ne récupère que les nouveaux matchs
- Mode **parallèle** (`--parallel`) : ThreadPoolExecutor pour multi-saisons
- Stockage en JSON dans `data/raw/nba/.../dt=YYYY-MM-DD/`

**Montrer dans MinIO :**
- Naviguer dans `datalake/data/raw/nba/balldontlie/games/` pour montrer les fichiers JSON partitionnés par date

---

## 4:00 – 5:30 | Traitement Spark (1 min 30)

> "Les données brutes sont ensuite transformées par Apache Spark."

**Montrer dans le terminal :**

```bash
# Montrer le job de formatage
head -40 jobs/format_balldontlie.py

# Montrer le coeur : combine_metrics.py
head -60 jobs/combine_metrics.py
```

**Points clés :**
- **Formatage** : JSON → Parquet (typage strict, compression snappy, columnar storage)
- **Normalisation** : noms d'équipes en minuscules/alphanumériques pour la jointure
- **Dédoublonnage** par `game_id` (gestion des overlaps multi-batch)
- **Jointure** entre balldontlie (matchs) et TheSportsDB (salles) via nom normalisé
- **Dédoublement home/away** : chaque match → 2 lignes (une par équipe)
- **Window functions** sur les 5 derniers matchs :
  - `win_rate_last5`, `avg_points_last5`, `wins_last5`
- **Rest days** : `lag()` sur la date du match précédent
- **Strength of Schedule** : fenêtre look-ahead de 5 matchs

**Montrer dans MinIO :**
- Naviguer dans `datalake/data/formatted/` puis `datalake/data/combined/` pour montrer les fichiers Parquet

---

## 5:30 – 6:30 | Modèle de Machine Learning (1 min)

> "À partir des KPIs calculés, nous entraînons un modèle de régression logistique."

**Montrer dans le terminal :**

```bash
# Montrer le script ML
cat jobs/train_predict.py
```

**Points clés :**
- **9 features** : avg_points_last5 (home/away), win_rate_last5 (home/away), rest_days (home/away), form_diff, points_diff, rest_diff
- **Régression logistique** PySpark ML, 20 itérations
- Valeurs manquantes (début de saison) remplies par 0
- **Sortie** : `win_probability_home` ∈ [0, 1] pour chaque match
- Modèle interprétable : le différentiel de forme (`form_diff`) est la feature la plus discriminante

---

## 6:30 – 7:30 | DAG Airflow (1 min)

> "L'ensemble du pipeline est orchestré par un DAG Airflow."

**Montrer :** Interface web Airflow à `http://localhost:8080`

```
Naviguer vers : DAGs → nba_pipeline → Graph View
```

**Points clés :**
- **8 tâches** organisées en DAG avec dépendances
- Deux branches parallèles d'ingestion qui convergent vers `combine_metrics`
- Exécution quotidienne (`@daily`)
- 3 modes : full run, incrémental, all-files
- Chaque tâche Spark soumise via `spark-submit` avec packages Hadoop-AWS

**Montrer dans l'UI :**
- Le graphe du DAG (vue Graph)
- Les logs d'une tâche réussie (cliquer sur une tâche → Logs)
- L'historique des exécutions (vue Grid/Gantt)

---

## 7:30 – 9:00 | Dashboard Kibana (1 min 30)

> "Les résultats sont visualisés dans un dashboard Kibana."

**Montrer :** Interface Kibana à `http://localhost:5601`

```bash
# Importer les visualisations pré-configurées
bash kibana/import_saved_objects.sh
```

**Naviguer dans les visualisations :**

1. **Win Rate par équipe** (Bar chart) — montrer les meilleures équipes
2. **Average Points last 5** (Bar chart) — puissance offensive
3. **Home vs Away Performance** (Pie chart) — avantage domicile
4. **Win Rate par conférence** (Bar chart) — Est vs Ouest
5. **Rest Days vs Win Rate** (Line chart) — impact du repos
6. **Distribution Win Probability** (Histogram) — calibration du modèle

**Points clés :**
- 2 index patterns : `nba_team_metrics` (18 champs), `nba_match_metrics` (9 champs)
- Filtres temporels par saison et par équipe
- Dashboard exportable/importable via `saved_objects.json`
- Montrer un filtre en live (ex : filtrer sur une équipe ou une période)

---

## 9:00 – 9:45 | Résultats et KPIs intéressants (45s)

> "Quelques résultats marquants de notre analyse."

**Montrer dans Kibana ou dans le rapport :**

- Le modèle prédit correctement ~65-70% des matchs (au-dessus du hasard à 50%)
- L'avantage domicile est confirmé par les données (win rate domicile > 55%)
- Le repos (rest days) a un impact mesurable sur la performance
- La forme récente (win_rate_last5) est le meilleur prédicteur

---

## 9:45 – 10:00 | Conclusion et perspectives (15s)

> "Pour conclure, ce projet démontre un pipeline big data complet et fonctionnel. En perspectives, nous envisageons d'intégrer Apache Kafka pour du temps réel, de tester des modèles plus expressifs comme XGBoost, et d'enrichir le Strength of Schedule avec le win rate des adversaires. Merci pour votre attention."

---

## Checklist avant l'enregistrement

- [ ] `docker-compose up -d` — tous les services sont UP
- [ ] Le DAG `nba_pipeline` a été exécuté au moins une fois avec succès
- [ ] Les index Elasticsearch contiennent des données (`curl http://localhost:9200/_cat/indices`)
- [ ] Le dashboard Kibana est importé (`bash kibana/import_saved_objects.sh`)
- [ ] Préparer les onglets du navigateur :
  - Airflow : `http://localhost:8080` (admin/admin)
  - MinIO : `http://localhost:9001` (minioadmin/minioadmin)
  - Kibana : `http://localhost:5601`
- [ ] Terminal ouvert dans le répertoire du projet
- [ ] Rapport PDF ouvert sur la page d'architecture

## Conseils pour l'enregistrement

- Utiliser un outil d'enregistrement d'écran (OBS Studio, Loom, ou QuickTime)
- Partager l'écran avec le terminal + navigateur côte à côte
- Parler calmement, ne pas se précipiter
- Si une démo plante, passer au point suivant (le contenu est plus important que la démo live)
