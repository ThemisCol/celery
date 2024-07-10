# Utiliza una imagen base oficial de Ubuntu
FROM ubuntu:20.04

# Configura las variables de entorno para el idioma español
ENV LANG=es_ES.UTF-8
ENV LANGUAGE=es_ES:es
ENV LC_ALL=es_ES.UTF-8


RUN apt-get update && \
    apt-get install -y locales && \
    locale-gen es_ES.UTF-8 && \
    update-locale LANG=es_ES.UTF-8

# Establece el directorio de trabajo
WORKDIR /app

# Configura las variables de entorno para evitar preguntas interactivas durante la instalación
ENV DEBIAN_FRONTEND=noninteractive

# Actualiza el sistema e instala dependencias necesarias
RUN apt-get update && \
    apt-get install -y \
        software-properties-common && \
    add-apt-repository universe && \
    apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        wget \
        unzip \
        libxt6 \
        libxext6 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Instala Flask, Celery y Pika
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copia el resto de la aplicación
COPY . .

# Copia el archivo de Matlab Runtime al contenedor
COPY MATLAB_Runtime_R2023b_Update_8_glnxa64.zip /tmp/matlab_runtime.zip

# Instala Matlab Runtime
RUN unzip /tmp/matlab_runtime.zip -d /tmp/matlab_runtime && \
    /tmp/matlab_runtime/install -mode silent -agreeToLicense yes && \
    rm -rf /tmp/matlab_runtime.zip /tmp/matlab_runtime

# Copia el código de TNC Project al contenedor
COPY TNCPROJECT.zip /tmp/tnc_project.zip

# Instala TNC Project
RUN unzip /tmp/tnc_project.zip -d /usr/src/ && \
     chmod -R 777 /usr/src/TNCPROJECT

# Expone el puerto 5000 para Flask
EXPOSE 5000

# Define el comando por defecto para ejecutar Flask
CMD ["bash", "-c", "flask run & celery -A app.celery worker --loglevel=info --concurrency=1"]
