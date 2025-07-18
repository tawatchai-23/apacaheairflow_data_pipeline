# # # Base image จาก Apache Airflow
# # FROM apache/airflow:2.10.5

# # # เปลี่ยนเป็น root เพื่อ install package
# # USER root

# # # ติดตั้ง system dependencies ที่ geopandas ต้องใช้
# # RUN apt-get update && apt-get install -y --no-install-recommends \
# #     g++ \
# #     gcc \
# #     libgdal-dev \
# #     libproj-dev \
# #     proj-bin \
# #     libgeos-dev \
# #     libspatialindex-dev \
# #     libsqlite3-dev \
# #     curl \
# #     ca-certificates \
# #  && apt-get clean \
# #  && rm -rf /var/lib/apt/lists/*

# # # กลับมาใช้ user airflow
# # USER airflow

# # # ติดตั้ง Python packages ที่จำเป็น
# # RUN pip install --no-cache-dir \
# #     geopandas \
# #     shapely \
# #     pyproj \
# #     fiona \
# #     pandas \
# #     packaging \
# #     psycopg2-binary

# # # ✅ ตรวจสอบว่า geopandas ติดตั้งสำเร็จ
# # RUN python -c "import geopandas; print('✅ GeoPandas installed OK')"


# FROM apache/airflow:2.10.5-python3.12

# USER root

# # ติดตั้ง system dependencies
# RUN apt-get update && \
#     apt-get install -y \
#         build-essential \
#         gdal-bin \
#         libgdal-dev \
#         libspatialindex-dev \
#         libgeos-dev \
#         libproj-dev \
#         python3-dev \
#         gcc \
#         && apt-get clean

# # ให้ Python รู้จัก gdal
# ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
# ENV C_INCLUDE_PATH=/usr/include/gdal

# # เปลี่ยนกลับมาใช้ user airflow ก่อนติดตั้ง Python package
# USER airflow

# # คัดลอก requirements.txt และติดตั้ง Python packages
# COPY --chown=airflow:airflow requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt


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


#     FROM apache/airflow:2.10.0-python3.9

# # ========= SYSTEM SETUP ========= #
# USER root

# # ติดตั้ง system dependencies
# RUN apt-get update && apt-get install -y --no-install-recommends \
#         build-essential \
#         gdal-bin \
#         libgdal-dev \
#         libspatialindex-dev \
#         libgeos-dev \
#         libproj-dev \
#         python3-dev \
#         gcc \
#         curl \
#         wget \
#         vim \
#         git \
#         netcat-openbsd \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# # ตั้งค่า GDAL environment
# ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
# ENV C_INCLUDE_PATH=/usr/include/gdal

# # ========= PYTHON DEPENDENCIES ========= #
# # กลับมาใช้ user airflow เพื่อ install Python packages
# USER airflow

# # อัปเกรด pip ก่อน
# RUN pip install --no-cache-dir --upgrade pip

# # คัดลอก requirements.txt แล้วติดตั้งแพ็กเกจทั้งหมด
# # นี่คือขั้นตอนเดียวที่จำเป็นสำหรับการติดตั้ง Python packages
# COPY --chown=airflow:airflow requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt