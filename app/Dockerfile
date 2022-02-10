FROM python:3.9 as builder
ENV PYTHONDONTWRITEBYTECODE 1
RUN apt-get update && apt-get -y install libmariadb3 libmariadb-dev
RUN pip install --upgrade pip
WORKDIR /app
COPY ./requirements.txt .
RUN pip install -r requirements.txt

FROM python:3.9-slim
ENV PYTHONDONTWRITEBYTECODE 1
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends apt-utils dialog 2>&1 \
      apt-utils \
      curl \
      libmariadb3 \
      libmariadb-dev \
      mariadb-client \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/lib/python3.9/site-packages/ /usr/local/lib/python3.9/site-packages/
WORKDIR /app
RUN groupadd --gid 9999 nhlapi \
    && useradd --home-dir /app \
        --uid 9999 \
        --gid 9999 --shell /bin/bash nhlapi
USER nhlusr
COPY ./nhlapi.py ./
COPY ./app.py ./
COPY ./static/ ./static/
COPY ./templates/ ./templates/
EXPOSE 5000
HEALTHCHECK --interval=20s --timeout=5s --retries=3 \
    CMD curl --fail http://localhost:5000/check/ || exit 1
CMD ["python", "-m", "flask", "run", "-h", "0.0.0.0"]
ENV DEBIAN_FRONTEND teletype