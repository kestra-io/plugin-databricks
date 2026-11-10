FROM python:3.11-slim
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-jdbc
LABEL org.opencontainers.image.description="Image with the latest databricks-sql-cli package"
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
# databricks-sql-cli pins pandas 1.3.4, which builds from source on 3.11 and needs pkg_resources (dropped in newer setuptools)
RUN echo "setuptools<81" > /tmp/build-constraints.txt && PIP_CONSTRAINT=/tmp/build-constraints.txt PIP_BUILD_CONSTRAINT=/tmp/build-constraints.txt pip install --no-cache databricks-sql-cli
RUN mkdir -p ~/.dbsqlcli && \
    echo "[main]" > ~/.dbsqlcli/dbsqlclirc && \
    echo "table_format = psql" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "smart_completion = True" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "multi_line = True" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "keyword_casing = auto" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "auto_retry = False" >> ~/.dbsqlcli/dbsqlclirc
RUN echo '#!/bin/sh\nexec "$@"' > /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]