FROM dvoros/sqoop:hive2-sqoop1.4.7

USER root

RUN find /usr/local/sqoop -type f -iname "postgresql*.jar" -delete || true && \
    find /usr/local/apache-hive-2.3.3-bin/lib -type f -iname "postgresql*.jar" -delete || true

COPY postgresql-42.7.7.jar /usr/local/sqoop/lib/postgresql-42.7.7.jar