from flask import Flask, request, jsonify
from celery import Celery, chain
import csv
import os
import logging
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sys
import glob

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'amqp://guest@rabbitmq//'
app.config['CELERY_RESULT_BACKEND'] = 'rpc://'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

smtp_server = ''
smtp_port = 587 
smtp_user = ''
smtp_password = ''

logging.basicConfig(level=logging.INFO)

def sendEmailOne(to, name):
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = to
    msg['Subject'] = 'Recepción de solicitud de análisis ' + name
    
    body = """
    <html>
    <head>
    <style>
    .email-container {
        font-family: Arial, sans-serif;
        background-color: #f4f4f4;
        padding: 20px;
    }
    .email-content {
        background-color: #ffffff;
        margin-top: 10px;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 0 10px rgba(0,0,0,0.1);
    }
    .email-footer {
        margin-top: 20px;
        font-size: 12px;
        color: #888888;
    }
    .left {
        float: left;
        width: auto;
        margin-right: 5%;
    }
    .right {
        float: right;
         width: auto;
        margin-left: 5%;
    }
     .clear {
        clear: both;
    }
    .content {
        display: block;
        clear: both;
        margin-top: 20px;
    }
    </style>
    </head>
    <body>
    <div class="email-container">
        <div class="email-content">
            <div>
                <img src="https://sima-dss.net/sites/default/files/styles/panopoly_image_original/public/logo-2%20%282%29.png" height="70" class="left">
                <img src="https://www.maps.tnc.org/nasca-dashboard/img/tnc-logo-light.1bb4846e.png" height="70" class="right">
            </div>
            <div class="clear"></div>
            <div class="content">
                <h1>Asistente de cálculo del índice de sostenibilidad</h1>
                <p>Se ha recibido su solicitud para el análisis. En breve, iniciaremos el proceso de modelación basado en los datos proporcionados.</p>
                <p>Le mantendremos informado sobre cualquier avance. </p>
            </div>
        </div>
        <div class="email-footer">
            El contenido de este correo y toda la información que contiene es confidencial y está protegido por las leyes de derechos de autor. Si usted no es el destinatario autorizado, por favor notifique al remitente y elimine cualquier copia de este mensaje y sus archivos adjuntos. Gracias.
        </div>
    </div>
    </body>
    </html>
    """
    msg.attach(MIMEText(body, 'html'))
    
    sendEmail(msg)

def sendEmail(msg):
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  
        server.login(smtp_user, smtp_password)
        
        text = msg.as_string()
        server.sendmail(smtp_user, msg['To'], text)
        
        print('Correo enviado exitosamente')
    except Exception as e:
        print(f'Error al enviar correo: {e}')
    finally:
        server.quit()

def create_csv(data_list, csv_file_path):
    if not data_list:
        logging.info(f"No data to write to CSV at {csv_file_path}")
        return
    
    headers = set()
    for item in data_list:
        headers.update(item.keys())
    
    headers = list(headers)
    
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        
        for record in data_list:
            row = [record.get(header, "") for header in headers]
            writer.writerow(row)
    logging.info(f"CSV file created at {csv_file_path}")

def create_csv_barriers(data_json, output_folder):
    logging.info(f"create_csv_barriers_task: output_folder = {output_folder}")

    if "data" not in data_json or not isinstance(data_json["data"], dict):
        logging.error("El JSON de entrada debe contener la clave 'data' como un diccionario.")
        raise ValueError("El JSON de entrada debe contener la clave 'data' como un diccionario.")

    data = data_json["data"]
    barrier_total = []

    for microcuenca_id, microcuenca_data in data.items():
        if not isinstance(microcuenca_data, dict):
            logging.warning(f"El valor para la clave {microcuenca_id} en 'data' no es un diccionario.")
            continue

        pointer_barrier_list = microcuenca_data.get("pointerBarrier", [])
        for barrier in pointer_barrier_list:
            cuenca_in = {
                'Codigo Microcuenca': microcuenca_id,
                'Nombre': barrier.get("name", ""),
                'Longitud': barrier.get("lng", ""),
                'Latitud': barrier.get("lat", ""),
            }
            barrier_total.append(cuenca_in)

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        logging.info(f"Created directory: {output_folder}")

    csv_file_path = os.path.join(output_folder, "Barriers.csv")
    logging.info(f"Creating CSV file at: {csv_file_path}")
    create_csv(barrier_total, csv_file_path)

    return data_json

def create_csv_WaterCatchments(input_json, output_folder):
    logging.info(f"create_csv_WaterCatchments_task: output_folder = {output_folder}")

    if not isinstance(input_json, dict):
        logging.error("El JSON de entrada debe ser un diccionario.")
        raise ValueError("El JSON de entrada debe ser un diccionario.")

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        logging.info(f"Created directory: {output_folder}")

    water_catchments_total = []
    for collection_key, collection_data in input_json.items():
        if isinstance(collection_data, dict):
            pointer_collection = collection_data.get("pointerCollection", [])
            for water in pointer_collection:
                catchment_in = {
                    'Codigo Microcuenca': collection_key,
                    'Nombre': water.get("name", ""),
                    'Longitud': water.get("lng", ""),
                    'Latitud': water.get("lat", ""),
                    'Caudal': water.get("cau", ""),
                }
                water_catchments_total.append(catchment_in)
        else:
            logging.warning(f"Ignoring non-dictionary value for key '{collection_key}'.")

    csv_file_path = os.path.join(output_folder, "WaterCatchments.csv")
    logging.info(f"Creating CSV file at: {csv_file_path}")
    create_csv(water_catchments_total, csv_file_path)

    return input_json


def create_csv_Pouring(data_json, output_folder):
    logging.info(f"create_csv_Pouring_task: output_folder = {output_folder}")
    try:
        input_json = data_json
        if not isinstance(input_json, dict):
            logging.error("Input JSON is not a dictionary.")
            raise ValueError("El JSON de entrada debe ser un diccionario.")

        # Continue processing as usual
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            logging.info(f"Created directory: {output_folder}")

        vertiment_total = []
        for pouring in input_json:
            if isinstance(input_json[pouring], dict):
                pointer_pouring = input_json[pouring].get("pointerPouring", [])
                for vertiment in pointer_pouring:
                    vertiment_in = {
                        'Codigo Microcuenca': pouring,
                        'Determinante': vertiment.get("name", ""),
                        'Longitud': vertiment.get("lng", ""),
                        'Latitud': vertiment.get("lat", ""),
                        'Caudal (m3/s)': vertiment.get("cau", ""),
                        'Solidos Suspendidos Totales (mg/l)': vertiment["data"].get("sst", ""),
                        'Coliformes Totales (NPM/100ml)': vertiment["data"].get("coliformes_totales", ""),
                        'Fosforo Organico (mg/l)': vertiment["data"].get("fosforo_organico", ""),
                        'Fosforo Inorganico (mg/l)': vertiment["data"].get("fosforo_inorganico", ""),
                        'Nitrogeno Organico (mg/l)': vertiment["data"].get("nitrogeno_organico", ""),
                        'Nitrogeno Amonical (mg/l)': vertiment["data"].get("nitrogeno_amoniacal", ""),
                        'Nitratos (mg/l)': vertiment["data"].get("nitratos", ""),
                        'DBO5 (mg/l)': vertiment["data"].get("dbo5", ""),
                        'Mercurio total (mg/l)': vertiment["data"].get("mercurio_total", ""),       
                    }
                    vertiment_total.append(vertiment_in)
            else:
                logging.warning(f"Skipping invalid data for pouring: {pouring}")

        csv_file_path = os.path.join(output_folder, "Dumpings.csv")
        logging.info(f"Creating CSV file at: {csv_file_path}")
        create_csv(vertiment_total, csv_file_path)
        return data_json
    
    except Exception as e:
        logging.error(f"Error processing JSON: {str(e)}")
        raise



def create_csv_basin(data_json, output_folder):
    logging.info(f"create_csv_basin_task: output_folder = {output_folder}")
    
    input_json = data_json.get("data", {})  # Accede a la clave 'data' del JSON
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        logging.info(f"Created directory: {output_folder}")

    basin_total = []
    
    for basin, basin_data in input_json.items():
        coverage = basin_data.get("coverage", [])
        pecuaria = basin_data.get("pecuaria", [])
        minera = basin_data.get("minera", [])
        population = basin_data.get("population", [])
        estresores = basin_data.get("estresores", [])

        headers = {'Codigo Microcuenca': basin}
        
        for cobe in coverage:
            headers.update({cobe["name"]: cobe["escA"]})
        for cobe in pecuaria:
            headers.update({cobe["name"]: cobe["pecB"]})
        for cobe in minera:
            headers.update({cobe["name"]: cobe["minB"]})
        for cobe in population:
            headers.update({cobe["name"]: cobe["popB"]})
        for cobe in estresores:
            headers.update({cobe["name"]: cobe["estB"]})
        
        basin_total.append(headers)
    
    csv_file_path = os.path.join(output_folder, "BasinData.csv")
    logging.info(f"Creating CSV file at: {csv_file_path}")
    create_csv(basin_total, csv_file_path)  # Asume que tienes una función 'create_csv' definida correctamente
    
    return data_json


@celery.task
def processAnalysis(data):
    timestamp = datetime.now().strftime('%Y-%m-%d_%H:%M')
    # 1. SendEmail Task
    sendEmailOne(data['correo'], timestamp)
    # 2. SendEmail Queue
    # ....
    # 3. Generate Folder and Control File
    output_folder = preparteData(timestamp)
    # 4. Generate CSV
    generateCSV(data, output_folder)
    # 5. MathLab Process
    # ....
    # 6. SendEmail End Task
    # ....

def preparteData(timestamp):
    base_output_folder = "/usr/src/TNCPROJECT/SIMA-PROJECT/UserData"  
    output_folder = os.path.join(base_output_folder, timestamp)
    # Create Folder
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        logging.info(f"Created timestamped directory: {output_folder}")
    # Create Control File
    base_output_file = "/usr/src/TNCPROJECT/Adapter_MATLAB" 
    input_file_name = "Control_File_MATLAB.txt" # file Guia
    with open(input_file_name, 'r') as file:
        content = file.read()
    modified_content = content.replace('{{folder_name}}', timestamp)
    
    file_name, file_extension = os.path.splitext(os.path.basename(input_file_name))
    new_file_name = f"{file_name}{file_extension}"
    existing_files = glob.glob(os.path.join(output_folder, f"{file_name}*"))
    for f in existing_files:
        os.remove(f)
        print(f"Deleted file: {f}")
    output_file_path = os.path.join(base_output_file, new_file_name)

    with open(output_file_path, 'w') as file:
        file.write(modified_content)

    return output_folder

def generateCSV(data_json, output_folder):
    create_csv_barriers(data_json, output_folder)
    create_csv_WaterCatchments(data_json, output_folder)
    create_csv_Pouring(data_json, output_folder)
    create_csv_basin(data_json, output_folder)


@app.route('/process_json', methods=['POST'])
def process_json():
    data = request.get_json()
    task = processAnalysis.delay(data)
    return jsonify({"task_id": task.id}), 202

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
