FROM python:3.12-slim
WORKDIR /app

# Install minimal build dependencies required by some Python packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
	   build-essential \
	   gcc \
	   libpq-dev \
	   curl \
	   ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

# Ensure pip/wheel/setuptools are up-to-date so wheels are preferred when available
RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY clients ./clients
COPY core ./core
COPY generated ./generated
COPY schemas ./schemas
COPY server ./server
COPY services ./services
COPY proto	./proto

EXPOSE 50051
CMD ["python", "-m", "server.main"]
