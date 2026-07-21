# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# spark/Dockerfile — Spark image (PySpark jobs + MinIO sync)
# =======================================================================

FROM apache/spark:3.5.1

USER root

COPY requirements.txt /tmp/spark-requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/spark-requirements.txt \
    && rm -f /tmp/spark-requirements.txt

COPY entrypoint.sh /usr/local/bin/edf-spark-entrypoint.sh
RUN chmod +x /usr/local/bin/edf-spark-entrypoint.sh

COPY download_jars.sh /tmp/download_jars.sh
RUN chmod +x /tmp/download_jars.sh && /tmp/download_jars.sh /opt/spark/extra-jars \
    && rm -f /tmp/download_jars.sh

ENV SPARK_EXTRA_JARS=/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/extra-jars/postgresql-42.7.4.jar

ENTRYPOINT ["/usr/local/bin/edf-spark-entrypoint.sh"]
