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
# Unprivileged user (fixed UID/GID 1000) with a writable HOME. The container itself starts as root
# and databricks-sql-cli-entrypoint.sh drops to this user: Kestra creates the task working directory as root.
RUN groupadd --gid 1000 kestra \
    && useradd --uid 1000 --gid kestra --create-home --home-dir /home/kestra --shell /bin/sh kestra
RUN mkdir -p /home/kestra/.dbsqlcli && \
    echo "[main]" > /home/kestra/.dbsqlcli/dbsqlclirc && \
    echo "table_format = psql" >> /home/kestra/.dbsqlcli/dbsqlclirc && \
    echo "smart_completion = True" >> /home/kestra/.dbsqlcli/dbsqlclirc && \
    echo "multi_line = True" >> /home/kestra/.dbsqlcli/dbsqlclirc && \
    echo "keyword_casing = auto" >> /home/kestra/.dbsqlcli/dbsqlclirc && \
    echo "auto_retry = False" >> /home/kestra/.dbsqlcli/dbsqlclirc && \
    chown -R kestra:kestra /home/kestra/.dbsqlcli
COPY --chmod=0755 dockerfiles/databricks-sql-cli-entrypoint.sh /usr/local/bin/kestra-entrypoint.sh
# SEC-0091: the container starts as root on purpose so kestra-entrypoint.sh can chown the root:root working dir
# created by the Kestra runner, then drops to UID 1000 with setpriv before running user commands.
# nosemgrep: dockerfile.security.missing-user-entrypoint.missing-user-entrypoint
ENTRYPOINT ["/usr/local/bin/kestra-entrypoint.sh"]
