FROM python:3.11-slim
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-jdbc
LABEL org.opencontainers.image.description="Image with the latest databricks-sql-cli package"
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
RUN pip install --no-cache databricks-sql-cli
RUN mkdir -p ~/.dbsqlcli && \
    echo "[main]" > ~/.dbsqlcli/dbsqlclirc && \
    echo "table_format = psql" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "smart_completion = True" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "multi_line = True" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "keyword_casing = auto" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "auto_retry = False" >> ~/.dbsqlcli/dbsqlclirc
RUN echo '#!/bin/sh\nexec "$@"' > /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]