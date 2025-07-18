FROM apache/airflow:2.10.5-python3.12

# ========= SYSTEM SETUP ========= #
USER root

# ติดตั้ง system dependencies
RUN apt-get update && apt-get install -y \
        build-essential \
        gdal-bin \
        libgdal-dev \
        libspatialindex-dev \
        libgeos-dev \
        libproj-dev \
        python3-dev \
        gcc \
        curl \
        wget \
        vim \
        git \
        netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ตั้งค่า GDAL environment
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# ========= PYTHON DEPENDENCIES ========= #
# กลับมาใช้ user airflow เพื่อ install Python packages
USER airflow

# คัดลอก requirements แล้วติดตั้ง
COPY --chown=airflow:airflow requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt
