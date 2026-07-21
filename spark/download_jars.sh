#!/bin/sh
# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/download_jars.sh — Pre-downloaded Maven dependencies - accessible on master and workers (cluster REST mode)
# =======================================================================

set -e

JARS_DIR="${1:-/opt/spark/extra-jars}"
mkdir -p "$JARS_DIR"

fetch() {
  url="$1"
  file="$2"
  if [ ! -f "$JARS_DIR/$file" ]; then
    echo "Downloading $file..."
    curl -fsSL "$url" -o "$JARS_DIR/$file"
  fi
}

fetch "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
  "hadoop-aws-3.3.4.jar"
fetch "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
  "aws-java-sdk-bundle-1.12.262.jar"
fetch "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" \
  "postgresql-42.7.4.jar"

chown -R spark:spark "$JARS_DIR"
