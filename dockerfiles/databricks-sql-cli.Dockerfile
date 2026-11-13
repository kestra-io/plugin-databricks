# Build stage: gcc / g++ / libc6-dev are only needed to compile pandas 1.3.4 (pinned by databricks-sql-cli, no wheel for
# Python 3.11), they don't ship in the final image.
FROM python:3.11-slim-trixie AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
# databricks-sql-cli pins pandas 1.3.4, which builds from source on 3.11 and needs pkg_resources (dropped in newer setuptools)
# pyarrow 26+ requires numpy 2, while the CLI pins numpy 1.23.4 (pyarrow doesn't declare numpy, so pip check can't see it)
RUN echo "setuptools<81" > /tmp/build-constraints.txt && PIP_CONSTRAINT=/tmp/build-constraints.txt PIP_BUILD_CONSTRAINT=/tmp/build-constraints.txt pip install --no-cache-dir databricks-sql-cli "pyarrow<26"
# thrift <0.17, oauthlib <4 and sqlparse <0.5 are pinned by databricks-sql-cli / databricks-sql-connector 2.5.2 and can't
# move; the other packages already resolve to their latest fixed versions.
RUN pip check

FROM python:3.11-slim-trixie
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-databricks
LABEL org.opencontainers.image.description="Image with the latest databricks-sql-cli package"
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local /usr/local
RUN mkdir -p ~/.dbsqlcli && \
    echo "[main]" > ~/.dbsqlcli/dbsqlclirc && \
    echo "table_format = psql" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "smart_completion = True" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "multi_line = True" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "keyword_casing = auto" >> ~/.dbsqlcli/dbsqlclirc && \
    echo "auto_retry = False" >> ~/.dbsqlcli/dbsqlclirc
RUN echo '#!/bin/sh\nexec "$@"' > /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
