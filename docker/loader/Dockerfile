# ---- OpenJDK image
FROM openjdk:18-jdk-alpine AS base

# Set JAVA_HOME
ENV JAVA_HOME /opt/openjdk-18

# ---- Inputs
ARG PYTHON_VERSION='3.9.12'

# ---- Update OS
RUN apk --update upgrade
RUN apk add --no-cache bash

# ---- Install Alpine packages
RUN apk add --no-cache \
  build-base \
  bzip2-dev \
  ca-certificates \
  curl \
  git \
  libffi-dev \
  libxslt-dev \
  linux-headers \
  ncurses-dev \
  openssl-dev \
  readline-dev \
  sqlite-dev \
  python3 \
  py3-pip

# Copy the requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script & data
COPY data.csv .
COPY main.py .

CMD ["python3", "main.py"]
