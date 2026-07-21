# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# Makefile — Interface de commandes
#
# 	make help               Liste des commandes et parcours
# 	make urls               Afficher les URLs et identifiants des services
# 	make init-data-dirs     Créer data/raw et data/eda (entrée XLS + graphiques persistés)
# 	make bootstrap          Workflow : build, stack, Kafka, schéma
#	make health             Contrôle de santé des services critiques
# 	make pipeline           Workflow : Airflow prod-like + résultats
# 	make pipeline-spark     ETL Spark direct (debug) + audit
# 	make run-xls-to-bronze  Job 1 — ingestion XLS → MinIO Bronze
# 	make run-bronze-to-silver Job 2 — nettoyage + features → Silver
# 	make run-silver-to-gold   Job 3 — agrégats → Gold
# 	make run-gold-to-model    Job 4 — entraînement ML (4 modèles)
# 	make run-etl              Jobs 1→3 + contrôles qualité
# 	make validate-xls-sources Trous d'années / plages XLS (pré-Bronze)
# 	make run-quality-checks   Contrôles Silver/Gold (post-ETL)
# 	make check-ml-readiness   Seuil ML_MIN_TRAINING_ROWS avant ML
# 	make run-ml               Job 4 uniquement
# 	make pipeline-spark     ETL Spark direct + qualité + ML
# 	make trigger-pipeline-wait Attendre edf_pipeline_complet
# 	make trigger-pipeline     Déclencher sans attendre
# 	make trigger-dag DAG=edf_pipeline_complet
# 	make trigger-dag DAG=edf_etl_pipeline
# 	make trigger-dag DAG=edf_ml_pipeline
# 	make trigger-dag DAG=edf_quality_monitoring
# 	make report-eda           Rapport complet (données + ML si entraîné)
# 	make report-eda-data      3 graphiques données (XLS)
# 	make report-eda-ml        Tuiles ML dashboard (après pipeline ML)
# 	make logs                 Afficher les logs des conteneurs
# 	make test                 Exécuter les tests
# 	make purge-local         Caches + artefacts locaux (garde XLS + Docker)
# 	make reset-zero            Purge locale + volumes Docker (from scratch)
# 	make shell-airflow        Console Airflow
# 	make shell-spark          Console Spark
# 	make shell-postgres       Console PostgreSQL
# =======================================================================

.DEFAULT_GOAL := help

# Variables d'environnement (.env) — credentials MinIO pipeline, chemins S3
-include .env
export

# =======================================================================
# CONFIGURATION
# =======================================================================

# Docker Compose
COMPOSE_FILE     := docker-compose.yml
DOCKER_COMPOSE   := docker-compose -f $(COMPOSE_FILE)

# Préfixe des conteneurs (cf. container_name dans docker-compose.yml)
CN_POSTGRES      := edf-postgres
CN_KAFKA         := edf-kafka
CN_SPARK         := edf-spark-master
CN_SPARK_WORKERS := edf-spark-master edf-spark-worker-1 edf-spark-worker-2
CN_AIRFLOW       := edf-airflow-webserver
CN_AIRFLOW_SCHED := edf-airflow-scheduler

# Endpoints & identifiants (environnement local) — alignés sur docker-compose.yml
URL_AIRFLOW      := http://localhost:8081
URL_SPARK_UI     := http://localhost:8082
URL_MINIO_CONSOLE:= http://localhost:9001
URL_KAFKA_UI     := http://localhost:8085
URL_GRAFANA      := http://localhost:3001

# PostgreSQL
PG_USER          := edf
PG_DB            := edf_dw

# Spark
SPARK_PYTHONPATH := /opt/spark-project
SPARK_MASTER     := spark://spark-master:7077
SPARK_MIN_WORKERS:= 2
MINIO_ENDPOINT   ?= http://minio:9000
MINIO_ACCESS_KEY ?= edfadmin
MINIO_SECRET_KEY ?= edfpassword123
SPARK_PKG_S3     := org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262
SPARK_PKG_JDBC   := org.postgresql:postgresql:42.7.4
SPARK_PACKAGES   := $(SPARK_PKG_S3),$(SPARK_PKG_JDBC)
SPARK_S3A_CONF    := \
	--conf spark.hadoop.fs.s3a.endpoint=$(MINIO_ENDPOINT) \
	--conf spark.hadoop.fs.s3a.access.key=$(MINIO_ACCESS_KEY) \
	--conf spark.hadoop.fs.s3a.secret.key=$(MINIO_SECRET_KEY) \
	--conf spark.hadoop.fs.s3a.path.style.access=true
SPARK_EXECUTOR_MEMORY         ?= 2g
SPARK_EXECUTOR_MEMORY_OVERHEAD?=768m
SPARK_DRIVER_MEMORY           ?= 5g
SPARK_SQL_SHUFFLE_PARTITIONS  ?= 4
SPARK_CORES_MAX               ?= 4
SPARK_ML_DRIVER_MEMORY         ?= 5g
SPARK_MEM_CONF := \
	--conf spark.executor.memory=$(SPARK_EXECUTOR_MEMORY) \
	--conf spark.executor.memoryOverhead=$(SPARK_EXECUTOR_MEMORY_OVERHEAD) \
	--conf spark.driver.memory=$(SPARK_DRIVER_MEMORY) \
	--conf spark.sql.shuffle.partitions=$(SPARK_SQL_SHUFFLE_PARTITIONS) \
	--conf spark.cores.max=$(SPARK_CORES_MAX)
SPARK_ML_MEM_CONF := \
	--conf spark.driver.memory=$(SPARK_ML_DRIVER_MEMORY) \
	--conf spark.executor.memory=$(SPARK_EXECUTOR_MEMORY) \
	--conf spark.sql.shuffle.partitions=$(SPARK_SQL_SHUFFLE_PARTITIONS)

# Airflow
DAG              ?= edf_pipeline_complet

# Affichage terminal
GREEN            := \033[0;32m
RED              := \033[0;31m
YELLOW           := \033[0;33m
BLUE             := \033[0;34m
CYAN             := \033[0;36m
NC               := \033[0m

# Require container (vérifie qu'un conteneur est en cours d'exécution).
define require-container
	@if [ -z "$$(docker ps -q -f name=$(1))" ]; then \
		printf "$(RED)[ERREUR]$(NC) Conteneur '$(1)' absent.\n"; \
		printf "         Exécutez : $(2)\n"; \
		exit 1; \
	fi
endef

# Spark submit (soumet un job PySpark sur le cluster).
define spark-submit
	$(call require-container,$(CN_SPARK),make up)
	@$(MAKE) --no-print-directory spark-ready
	@docker exec -u spark -e PYTHONPATH=$(SPARK_PYTHONPATH) $(CN_SPARK) /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER) \
		--packages $(SPARK_PACKAGES) \
		--conf spark.jars.ivy=/home/spark/.ivy2 \
		--conf spark.driverEnv.PYTHONPATH=$(SPARK_PYTHONPATH) \
		--conf spark.executorEnv.PYTHONPATH=$(SPARK_PYTHONPATH) \
		$(SPARK_MEM_CONF) \
		$(SPARK_S3A_CONF) \
		$(1)
endef

# Spark submit ML (soumet un job PySpark sur le cluster).
define spark-submit-ml
	$(call require-container,$(CN_SPARK),make up)
	@$(MAKE) --no-print-directory spark-ready
	@docker exec -u spark \
		-e PYTHONPATH=$(SPARK_PYTHONPATH) \
		-e POSTGRES_CONN=$${POSTGRES_CONN:-postgresql://edf:edf123@postgres:5432/edf_dw} \
		$(CN_SPARK) /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER) \
		--packages $(SPARK_PACKAGES) \
		--conf spark.jars.ivy=/home/spark/.ivy2 \
		--conf spark.driverEnv.PYTHONPATH=$(SPARK_PYTHONPATH) \
		--conf spark.executorEnv.PYTHONPATH=$(SPARK_PYTHONPATH) \
		--conf spark.driverEnv.POSTGRES_CONN=$${POSTGRES_CONN:-postgresql://edf:edf123@postgres:5432/edf_dw} \
		$(SPARK_ML_MEM_CONF) \
		$(SPARK_S3A_CONF) \
		$(1)
endef

# Pipeline header (affiche l'en-tête d'une étape pipeline).
define pipeline-header
	@printf "\n$(CYAN)▶ $(1)$(NC)\n"
endef

# =======================================================================
# DÉCLARATION DES CIBLES (.PHONY)
# =======================================================================

.PHONY: help urls bootstrap pipeline pipeline-spark record-pipeline-run \
        build up down down-force restart restart-airflow restart-spark status health kafka-ready init-kafka \
        init-postgres-schema init-ml-schema ensure-spark-pool ivy-ready spark-ready validate-xls-sources \
        run-quality-checks check-ml-readiness \
        run-xls-to-bronze run-bronze-to-silver run-silver-to-gold \
        run-gold-to-model \
        run-etl run-ml \
        init-data-dirs trigger-dag trigger-pipeline trigger-pipeline-wait \
        report-eda report-eda-data report-eda-ml sync-report-eda-minio sync-models-local ml-metrics logs \
        test purge-local reset-zero clean shell-airflow shell-spark shell-postgres help-section

# =======================================================================
# AIDE & WORKFLOW PRINCIPAL
# =======================================================================

help: ## Afficher l'aide et le parcours d'exécution
	@printf "$(GREEN)EDF ETL Platform$(NC) — Makefile\n\n"
	@printf "$(BLUE)Parcours standard$(NC)\n"
	@printf "  1. cp eCO2mix_RTE_*.xls ./data/raw/\n"
	@printf "  2. make bootstrap\n"
	@printf "  3. make health\n"
	@printf "  4. make pipeline           # Airflow prod-like (qualité + ETL + ML)\n"
	@printf "     make pipeline-spark     # Spark direct (debug rapide)\n\n"
	@printf "$(BLUE)Commandes$(NC) — $(CYAN)make help$(NC) pour cette liste complète\n"
	@$(MAKE) --no-print-directory help-section SECTION="Workflow" FILTER='^(bootstrap|pipeline|pipeline-spark|reset-zero)$$'
	@$(MAKE) --no-print-directory help-section SECTION="Infrastructure Docker" FILTER='^(build|up|down|down-force|restart|restart-airflow|restart-spark|status|health|clean|urls|init-data-dirs|init-kafka|init-postgres-schema|ensure-spark-pool|ivy-ready|spark-ready|kafka-ready)$$'
	@$(MAKE) --no-print-directory help-section SECTION="Pipelines Spark" FILTER='^(run-xls-to-bronze|run-bronze-to-silver|run-silver-to-gold|run-gold-to-model|run-etl|run-ml|validate-xls-sources|run-quality-checks|check-ml-readiness|record-pipeline-run)$$'
	@$(MAKE) --no-print-directory help-section SECTION="Airflow" FILTER='^(trigger-dag|trigger-pipeline|trigger-pipeline-wait)$$'
	@$(MAKE) --no-print-directory help-section SECTION="Rapports & résultats" FILTER='^(report-eda|report-eda-data|report-eda-ml|sync-report-eda-minio|sync-models-local|ml-metrics|init-ml-schema|logs)$$'
	@$(MAKE) --no-print-directory help-section SECTION="Développement & maintenance" FILTER='^(test|dev-venv|purge-local|shell-airflow|shell-spark|shell-postgres)$$'

.PHONY: help-section
help-section:
	@printf "\n$(BLUE)$(SECTION)$(NC)\n"
	@grep -hE '^[a-zA-Z0-9_-]+.*:.*## ' $(CURDIR)/Makefile \
		| sed -E 's/:.*## /|/' \
		| grep -E '^($(FILTER))\|' \
		| sort -t'|' -k1 \
		| awk -F'|' '{printf "  $(YELLOW)%-24s$(NC) %s\n", $$1, $$2}'

urls: ## Afficher les URLs et identifiants des services
	@printf "$(BLUE)Interfaces web$(NC)\n"
	@printf "  Webapp       http://localhost:5000\n"
	@printf "  Airflow      $(URL_AIRFLOW)        admin / admin123\n"
	@printf "  Spark        $(URL_SPARK_UI)\n"
	@printf "  MinIO        $(URL_MINIO_CONSOLE)      edfadmin / edfpassword123\n"
	@printf "  Kafka UI     $(URL_KAFKA_UI)\n"
	@printf "  Grafana      $(URL_GRAFANA)        admin / edf-admin\n"
	@printf "  Prometheus   http://localhost:9090\n"
	@printf "\n$(BLUE)Base de données$(NC)\n"
	@printf "  PostgreSQL   localhost:5432 / $(PG_DB)  ($(PG_USER) / edf123)\n"

init-data-dirs: ## Créer data/raw, data/eda/report et data/models/rte
	@mkdir -p data/raw data/eda/report data/models/rte
	@printf "$(GREEN)[OK]$(NC) Arborescence data/ prête (raw + eda/report + models/rte).\n"

bootstrap: init-data-dirs build up init-kafka init-postgres-schema ensure-spark-pool health urls ## [Workflow] Étape 1 — dirs, build, stack, Kafka, schéma
	@printf "\n$(GREEN)[OK]$(NC) Environnement prêt. Prochaine étape : $(YELLOW)make pipeline$(NC)\n"

pipeline: trigger-pipeline-wait ml-metrics ## [Workflow] Étape 2 — Airflow prod-like (EDA orchestré par le DAG)
	@printf "\n$(GREEN)[OK]$(NC) Pipeline Airflow terminé.\n"

pipeline-spark: run-etl run-ml ml-metrics record-pipeline-run ## ETL Spark direct (debug) + rapport
	@printf "\n$(GREEN)[OK]$(NC) Pipeline Spark terminé.\n"

# --- Qualité données (Makefile = même logique qu'Airflow edf_pipeline_complet) ---
QUALITY_FAIL_ON_WARNING ?= false

AIRFLOW_EXEC = docker exec \
	-e POSTGRES_CONN=postgresql://$(PG_USER):edf123@postgres:5432/$(PG_DB) \
	-e DATA_DIR=/opt/airflow/data/raw \
	-e PYTHONPATH=/opt/airflow/project:/opt/airflow/plugins \
	-e QUALITY_FAIL_ON_WARNING=$(QUALITY_FAIL_ON_WARNING) \
	$(CN_AIRFLOW)

validate-xls-sources: ## Contrôles sources XLS (plages dates, trous d'années) avant Bronze
	$(call require-container,$(CN_AIRFLOW),make up)
	@printf "$(BLUE)Contrôles sources XLS$(NC)\n"
	@$(AIRFLOW_EXEC) python -c "from edf_pipeline.quality import validate_xls_sources_or_raise; validate_xls_sources_or_raise()"

run-quality-checks: ## Contrôles qualité post-ETL PostgreSQL (bloquant si échec critique)
	$(call require-container,$(CN_POSTGRES),make up)
	@printf "$(BLUE)Contrôles qualité post-ETL$(NC)\n"
	@$(AIRFLOW_EXEC) python -c "from edf_pipeline.quality import run_post_etl_quality_or_raise; run_post_etl_quality_or_raise('make pipeline-spark')"

init-postgres-schema: ## Appliquer schema_dw.sql (idempotent)
	$(call require-container,$(CN_AIRFLOW),make up)
	@printf "$(BLUE)Initialisation schéma PostgreSQL$(NC)\n"
	@$(AIRFLOW_EXEC) python -c "from edf_pipeline.schema import init_postgres_schema; init_postgres_schema()"

ensure-spark-pool: ## Créer/mettre à jour le pool Airflow spark_cluster (1 slot)
	$(call require-container,$(CN_AIRFLOW),make up)
	@printf "$(BLUE)Pool Airflow Spark (mode professionnel)$(NC)\n"
	@$(AIRFLOW_EXEC) python -c "from edf_pipeline.pools import ensure_spark_pool; print(ensure_spark_pool())"

check-ml-readiness: ## Vérifier ML_MIN_TRAINING_ROWS avant entraînement ML
	$(call require-container,$(CN_POSTGRES),make up)
	@$(AIRFLOW_EXEC) python -c "from edf_pipeline.ml_readiness import ensure_ml_ready_or_raise; n=ensure_ml_ready_or_raise(); print(f'ML-ready: {n} lignes Silver')"


# =======================================================================
# INFRASTRUCTURE DOCKER
# =======================================================================

build: ## Construire les images Airflow et Spark
	@printf "$(GREEN)[BUILD]$(NC) Image Airflow...\n"
	docker build -f airflow/Dockerfile -t edf-airflow:latest ./airflow
	@printf "$(GREEN)[BUILD]$(NC) Image Spark...\n"
	docker build -f spark/Dockerfile -t edf-spark:3.5.1 ./spark
	@printf "$(GREEN)[OK]$(NC) Images prêtes.\n"

up: ## Démarrer tous les services Docker
	$(DOCKER_COMPOSE) up -d
	@$(MAKE) --no-print-directory urls

down: ## Arrêter tous les conteneurs (conserve les volumes)
	$(DOCKER_COMPOSE) down --timeout 60 || { \
		printf "$(YELLOW)[WARN]$(NC) Arrêt normal échoué (processus zombie?) — nettoyage forcé...\n"; \
		containers=$$(docker ps -aq --filter "name=edf-"); \
		if [ -n "$$containers" ]; then docker rm -f $$containers 2>/dev/null || true; fi; \
		$(DOCKER_COMPOSE) down --remove-orphans --timeout 10; \
	}

down-force: ## Arrêt forcé si Airflow ne s'arrête pas (bug Docker Desktop)
	-docker rm -f $(CN_AIRFLOW) $(CN_AIRFLOW_SCHED) 2>/dev/null
	$(DOCKER_COMPOSE) down --remove-orphans --timeout 10

restart: down up ## Redémarrer la stack complète

restart-airflow: ## Redémarrer Airflow (évite bug Docker Desktop « did not receive an exit event »)
	@printf "$(YELLOW)[AIRFLOW]$(NC) Suppression forcée webserver + scheduler...\n"
	-docker rm -f $(CN_AIRFLOW) $(CN_AIRFLOW_SCHED) 2>/dev/null
	$(DOCKER_COMPOSE) up -d airflow-webserver airflow-scheduler
	@printf "$(GREEN)[OK]$(NC) Airflow relancé (attendre ~1 min healthcheck webserver).\n"

restart-spark: ## Redémarrer master + workers (ré-enregistrement cluster)
	@printf "$(YELLOW)[SPARK]$(NC) Redémarrage master puis workers ($(SPARK_WORKER_MEMORY), executor $(SPARK_EXECUTOR_MEMORY))...\n"
	$(DOCKER_COMPOSE) up -d spark-master spark-worker-1 spark-worker-2
	@sleep 10
	@printf "$(BLUE)[SPARK]$(NC) Attente enregistrement workers"
	@workers=0; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12; do \
		workers=$$(curl -sf $(URL_SPARK_UI)/json/ 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('aliveworkers',0))" 2>/dev/null || echo 0); \
		[ "$$workers" -ge $(SPARK_MIN_WORKERS) ] && break; \
		printf "."; sleep 5; \
	done; printf "\n"; \
	if [ "$$workers" -lt $(SPARK_MIN_WORKERS) ]; then \
		printf "$(RED)[ERREUR]$(NC) Spark : $$workers worker(s) après redémarrage.\n"; \
		exit 1; \
	fi; \
	printf "$(GREEN)[OK]$(NC) Spark cluster : $$workers worker(s).\n"

status: ## Afficher l'état des conteneurs
	@$(DOCKER_COMPOSE) ps

health: ## Contrôle de santé des services critiques
	@printf "$(BLUE)Contrôle de santé$(NC)\n"
	@printf "  PostgreSQL : " \
		&& (docker exec $(CN_POSTGRES) pg_isready -U $(PG_USER) -q 2>/dev/null \
		    && printf "$(GREEN)OK$(NC)\n" || printf "$(RED)FAIL$(NC)\n")
	@printf "  MinIO      : " \
		&& (curl -sf http://localhost:9000/minio/health/live >/dev/null \
		    && printf "$(GREEN)OK$(NC)\n" || printf "$(RED)FAIL$(NC)\n")
	@printf "  Kafka      : " \
		&& (docker exec $(CN_KAFKA) sh -c 'nc -z localhost 9092' 2>/dev/null \
		    && printf "$(GREEN)OK$(NC)\n" || printf "$(RED)FAIL$(NC)\n")
	@printf "  Spark      : " \
		&& (workers=$$(curl -sf $(URL_SPARK_UI)/json/ 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('aliveworkers',0))" 2>/dev/null || echo 0) \
		    && [ "$$workers" -ge $(SPARK_MIN_WORKERS) ] \
		    && printf "$(GREEN)OK$(NC) ($$workers workers)\n" \
		    || printf "$(RED)FAIL$(NC) ($${workers:-0} workers — make restart-spark)\n")
	@printf "  Airflow    : " \
		&& (ok=0; \
		    for i in $$(seq 1 36); do \
		      curl -sf $(URL_AIRFLOW)/health >/dev/null 2>&1 && ok=1 && break; \
		      sleep 10; \
		    done; \
		    [ "$$ok" = "1" ] \
		    && printf "$(GREEN)OK$(NC)\n" \
		    || printf "$(RED)FAIL$(NC) (webserver encore en démarrage — docker logs $(CN_AIRFLOW))\n")
	@printf "  Grafana    : " \
		&& (curl -sf $(URL_GRAFANA)/api/health >/dev/null \
		    && printf "$(GREEN)OK$(NC)\n" || printf "$(RED)FAIL$(NC)\n")

ivy-ready: ## Préparer le cache Ivy (master + workers) pour spark.jars.packages
	@for c in $(CN_SPARK_WORKERS); do \
		if docker ps -q -f name=$$c | grep -q .; then \
			docker exec -u 0 $$c sh -c 'mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chown -R 185:185 /home/spark/.ivy2' 2>/dev/null || true; \
		fi; \
	done

spark-ready: ivy-ready ## Vérifier / rétablir l'enregistrement des workers Spark
	$(call require-container,$(CN_SPARK),make up)
	@workers=$$(curl -sf $(URL_SPARK_UI)/json/ 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('aliveworkers',0))" 2>/dev/null || echo 0); \
	if [ "$$workers" -lt $(SPARK_MIN_WORKERS) ]; then \
		printf "$(YELLOW)[WARN]$(NC) Spark : $$workers worker(s) — redémarrage cluster...\n"; \
		$(MAKE) --no-print-directory restart-spark; \
	else \
		printf "$(GREEN)[OK]$(NC) Spark cluster : $$workers worker(s).\n"; \
	fi

# Kafka 
kafka-ready: ## Attendre que le broker Kafka accepte les commandes admin
	$(call require-container,$(CN_KAFKA),make up)
	@printf "$(BLUE)[KAFKA]$(NC) Attente broker prêt"
	@ready=0; \
	for i in $$(seq 1 60); do \
		if docker exec $(CN_KAFKA) kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
			ready=1; break; \
		fi; \
		printf "."; sleep 10; \
	done; printf "\n"; \
	if [ "$$ready" != "1" ]; then \
		printf "$(RED)[ERREUR]$(NC) Kafka broker indisponible après ~10 min (logs: docker logs $(CN_KAFKA)).\n"; \
		exit 1; \
	fi; \
	printf "$(GREEN)[OK]$(NC) Kafka broker prêt.\n"

init-kafka: kafka-ready ## Créer les topics Kafka (rte.raw, rte.tempo, rte.realtime)
	@printf "$(BLUE)[KAFKA]$(NC) Création des topics...\n"
	@for spec in rte.raw:3 rte.tempo:1 rte.realtime:3; do \
		topic=$${spec%%:*}; parts=$${spec##*:}; \
		ok=0; \
		for attempt in 1 2 3 4 5; do \
			if docker exec $(CN_KAFKA) kafka-topics --create --if-not-exists \
				--bootstrap-server localhost:9092 \
				--topic $$topic --partitions $$parts --replication-factor 1; then \
				ok=1; break; \
			fi; \
			printf "$(YELLOW)[KAFKA]$(NC) Retry $$topic ($$attempt/5)...\n"; \
			sleep 5; \
		done; \
		if [ "$$ok" != "1" ]; then \
			printf "$(RED)[ERREUR]$(NC) Impossible de créer le topic $$topic\n"; \
			exit 1; \
		fi; \
	done
	@printf "$(GREEN)[OK]$(NC) Topics : rte.raw, rte.tempo, rte.realtime\n"

# =======================================================================
# PIPELINES SPARK
# =======================================================================

run-xls-to-bronze: validate-xls-sources
run-xls-to-bronze: SPARK_PACKAGES := $(SPARK_PKG_S3)
run-xls-to-bronze: ## Job 1/4 — XLS → Bronze (MinIO)
	$(call pipeline-header,Job 1/4 — XLS → Bronze)
	$(call spark-submit,/opt/spark-jobs/xls_to_bronze.py /opt/spark-data/raw)

run-bronze-to-silver: ## Job 2/4 — Bronze → Silver
	$(call pipeline-header,Job 2/4 — Bronze → Silver)
	$(call spark-submit,/opt/spark-jobs/bronze_to_silver.py)

run-silver-to-gold: ## Job 3/4 — Silver → Gold
	$(call pipeline-header,Job 3/4 — Silver → Gold)
	$(call spark-submit,/opt/spark-jobs/silver_to_gold.py)

run-gold-to-model: ## Job 4/4 — Gold → ML (cluster Spark, 4 modèles)
	$(call pipeline-header,Job 4/4 — Gold → Modeling)
	$(call spark-submit-ml,/opt/spark-jobs/gold_to_model.py)

run-etl: run-xls-to-bronze run-bronze-to-silver run-silver-to-gold run-quality-checks report-eda-data ## Chaîne ETL jobs 1→3 + qualité + rapport données

run-ml: check-ml-readiness run-gold-to-model report-eda-ml ## Pipeline ML uniquement (seuil Silver) + rapport ML


# =======================================================================
# ORCHESTRATION AIRFLOW
# =======================================================================

trigger-dag: ## Déclencher un DAG Airflow (DAG=<nom>, sans attente)
	$(call require-container,$(CN_AIRFLOW),make up)
	@docker exec $(CN_AIRFLOW) airflow dags trigger $(DAG)
	@printf "$(GREEN)[OK]$(NC) DAG '$(DAG)' déclenché — suivi : $(URL_AIRFLOW)\n"

trigger-pipeline: ## Déclencher edf_pipeline_complet sans attendre
	@$(MAKE) trigger-dag DAG=edf_pipeline_complet

trigger-pipeline-wait: ivy-ready spark-ready ## Déclencher edf_pipeline_complet et attendre la fin (prod-like)
	$(call require-container,$(CN_AIRFLOW),make up)
	@printf "$(BLUE)Pipeline Airflow$(NC) — $(DAG) (attente jusqu'à 8h)\n"
	@$(AIRFLOW_EXEC) python -c "from edf_pipeline.wait_dag import trigger_and_wait; trigger_and_wait('$(DAG)')"
	@printf "$(GREEN)[OK]$(NC) DAG '$(DAG)' terminé avec succès.\n"


# =======================================================================
# RÉSULTATS & OBSERVABILITÉ
# =======================================================================

report-eda-data: ## Rapport pro : consommation + mensuel + qualité — CLI manuel
	@printf "$(BLUE)Rapport EDA — données$(NC) → ./data/eda/report/ + MinIO rapport-eda\n"
	@python3 -c "import pandas, matplotlib, boto3" 2>/dev/null || pip3 install -q pandas matplotlib boto3 psycopg2-binary
	@EDF_PROJECT_ROOT=$(CURDIR) DATA_DIR=$(CURDIR)/data/raw MODEL_LOCAL_PATH=data/models/rte \
		MINIO_ENDPOINT=http://localhost:9000 \
		POSTGRES_CONN=postgresql://$(PG_USER):edf123@localhost:5432/$(PG_DB) \
		PYTHONPATH=. python3 scripts/generate_report_eda.py --data-only

report-eda-ml: init-ml-schema ## Rapport pro : tuiles ML dashboard — CLI manuel
	@printf "$(BLUE)Rapport EDA — ML$(NC) → ./data/eda/report/ + MinIO rapport-eda\n"
	@python3 -c "import pandas, matplotlib, pyarrow, boto3" 2>/dev/null || pip3 install -q pandas matplotlib pyarrow boto3 psycopg2-binary
	@EDF_PROJECT_ROOT=$(CURDIR) DATA_DIR=$(CURDIR)/data/raw MODEL_LOCAL_PATH=data/models/rte \
		MINIO_ENDPOINT=http://localhost:9000 \
		POSTGRES_CONN=postgresql://$(PG_USER):edf123@localhost:5432/$(PG_DB) \
		PYTHONPATH=. python3 scripts/generate_report_eda.py --ml-only

sync-report-eda-minio: ## Pousser les PNG locaux data/eda/report/ → MinIO rapport-eda
	@python3 -c "import boto3" 2>/dev/null || pip3 install -q boto3
	@EDF_PROJECT_ROOT=$(CURDIR) MINIO_ENDPOINT=http://localhost:9000 PYTHONPATH=. python3 -c "\
from pathlib import Path; \
from spark.common.config import REPORT_EDA_LOCAL, REPORT_EDA_S3_PREFIX; \
from spark.common.eda_specs import all_report_filenames; \
from spark.common.object_storage import sync_report_pngs; \
root=Path(REPORT_EDA_LOCAL); \
root=root if root.is_absolute() else Path('$(CURDIR)')/REPORT_EDA_LOCAL; \
names=all_report_filenames(); \
local=[n for n in names if (root/n).is_file()]; \
uris=sync_report_pngs(root, names); \
prefix=REPORT_EDA_S3_PREFIX.strip('/'); \
dest=f's3a://rapport-eda/{prefix}/' if prefix else 's3a://rapport-eda/'; \
print(f'Sync MinIO : {len(uris)}/{len(names)} PNG ({len(local)} local) → {dest}')"

report-eda: report-eda-data report-eda-ml ## Rapport pro complet (ML en attente si non entraîné)
	@count=$$(find data/eda/report -maxdepth 1 -name '*.png' 2>/dev/null | wc -l | tr -d ' '); \
		if [ -f data/eda/report/ml_dashboard.pending ]; then \
			printf "$(YELLOW)⏳$(NC) ML en attente — lancez $(YELLOW)make run-ml$(NC) puis $(YELLOW)make report-eda-ml$(NC)\n"; \
		fi; \
		printf "$(GREEN)✓$(NC) %s graphique(s) PNG dans data/eda/report/\n" "$$count"

init-ml-schema: ## Créer etl.model_metrics si absent (bases déjà initialisées)
	$(call require-container,$(CN_AIRFLOW),make up)
	@$(AIRFLOW_EXEC) python -c "from edf_pipeline.schema import ensure_model_metrics_schema; ensure_model_metrics_schema()"
	@printf "$(GREEN)✓$(NC) Schéma etl.model_metrics prêt\n"

record-pipeline-run: ## Enregistrer pipeline-spark dans etl.pipeline_runs (Grafana)
	$(call require-container,$(CN_POSTGRES),make up)
	@$(AIRFLOW_EXEC) python -c "from edf_pipeline.metadata import record_spark_batch_run; record_spark_batch_run()"
	@printf "$(GREEN)✓$(NC) Run enregistré dans etl.pipeline_runs\n"

sync-models-local: init-data-dirs ## Télécharger models/ MinIO → data/models/rte/
	@python3 -c "import boto3" 2>/dev/null || pip3 install -q boto3
	@EDF_PROJECT_ROOT=$(CURDIR) MODEL_LOCAL_PATH=data/models/rte PYTHONPATH=. python3 -c "\
from spark.common.object_storage import sync_model_artifacts, sync_ml_report_artifacts; \
from spark.common.config import resolve_model_local_path; \
n=sync_model_artifacts(); \
r=sync_ml_report_artifacts(); \
print(f'Sync OK — {n} modèle(s), {r} artefact(s) rapport → {resolve_model_local_path()}')"

ml-metrics: init-ml-schema ## Afficher les métriques ML (etl.model_metrics)
	@docker exec $(CN_POSTGRES) psql -U $(PG_USER) -d $(PG_DB) -c "\
		SELECT trained_at, model_name, \
		       ROUND(rmse::numeric,2)  AS rmse, \
		       ROUND(mae::numeric,2)   AS mae, \
		       ROUND(r2::numeric,3)    AS r2, \
		       ROUND(mape_pct::numeric,2) AS mape_pct \
		FROM etl.model_metrics \
		ORDER BY trained_at DESC \
		LIMIT 20;" \
		2>/dev/null \
		|| printf "$(YELLOW)[INFO]$(NC) Table etl.model_metrics absente — exécutez : make run-ml\n"

logs: ## Logs agrégés de tous les services
	$(DOCKER_COMPOSE) logs -f --tail=100


# =======================================================================
# DÉVELOPPEMENT & MAINTENANCE
# =======================================================================

# Python local pour les tests (venv du projet, sinon python3 système)
PYTHON           := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PIP              := $(if $(wildcard .venv/bin/pip),.venv/bin/pip,pip3)

test: ## Tests unitaires (pytest — utilise .venv si présent)
	$(PYTHON) -m pytest tests/ -v

dev-venv: ## Crée .venv et installe requirements-test.txt (numpy/pyarrow épinglés)
	rm -rf .venv
	python3 -m venv .venv --clear
	$(PIP) install -U pip
	$(PIP) install -r requirements-test.txt
	@printf "$(GREEN)[OK]$(NC) Environnement prêt. Lancez : $(CYAN)make test$(NC)\n"

purge-local: ## Caches Python, logs Airflow et artefacts EDA/ML locaux (sans volumes Docker)
	@printf "$(YELLOW)Nettoyage local…$(NC)\n"
	@find . \( -path './.venv' -o -path './.git' \) -prune -o -type d -name '__pycache__' -print0 2>/dev/null | xargs -0 rm -rf 2>/dev/null || true
	@find . \( -path './.venv' -o -path './.git' \) -prune -o -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete 2>/dev/null || true
	@rm -rf .pytest_cache spark/logs
	@find . -name '.DS_Store' -delete 2>/dev/null || true
	@mkdir -p airflow/logs
	@find airflow/logs -mindepth 1 -delete 2>/dev/null || true
	@mkdir -p data/eda/report data/models data/raw
	@find data/eda/report -maxdepth 1 -type f \( -name '*.png' -o -name '*.pending' \) -delete 2>/dev/null || true
	@find data/models -mindepth 1 -delete 2>/dev/null || true
	@touch data/eda/report/.gitkeep data/models/.gitkeep
	@printf "$(GREEN)[OK]$(NC) Projet local remis à zéro (données XLS dans data/raw conservées).\n"

reset-zero: ## Remise à zéro complète : purge locale + volumes Docker (relancer make bootstrap)
	@$(MAKE) purge-local
	@$(MAKE) clean
	@printf "$(GREEN)[OK]$(NC) Environnement prêt pour un démarrage from scratch.\n"
	@printf "  1. $(CYAN)make bootstrap$(NC)\n"
	@printf "  2. $(CYAN)make health$(NC)\n"
	@printf "  3. $(CYAN)make pipeline$(NC)  ou  $(CYAN)make pipeline-spark$(NC)\n"

clean: ## Arrêter la stack et supprimer les volumes Docker (MinIO, Postgres, caches Spark)
	@printf "$(RED)[ATTENTION]$(NC) Suppression des volumes Docker.\n"
	$(DOCKER_COMPOSE) down -v --remove-orphans

shell-airflow: ## Shell dans le conteneur Airflow
	@docker exec -it $(CN_AIRFLOW) bash

shell-spark: ## Shell dans Spark Master
	@docker exec -it $(CN_SPARK) bash

shell-postgres: ## Console psql PostgreSQL
	@docker exec -it $(CN_POSTGRES) psql -U $(PG_USER) -d $(PG_DB)
