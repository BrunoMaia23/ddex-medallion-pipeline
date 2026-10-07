# Roda a demo sem instalar Java nem Python na máquina:
#   docker build -t ddexpipe .
#   docker run --rm ddexpipe
FROM eclipse-temurin:17-jre-noble

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv \
 && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .

# baixa o runtime do Iceberg durante o build: rodar a imagem não depende de internet
RUN python -c "from ddexpipe.spark import aquecer; aquecer()"

# Spark em modo local: o driver escuta só no loopback
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /dados
CMD ["ddexpipe", "demo", "--base", "/dados/demo"]
