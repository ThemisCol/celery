from flask import Flask, request, jsonify
from celery import Celery, chain
import csv
import os

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'amqp://guest@rabbitmq//'
app.config['CELERY_RESULT_BACKEND'] = 'rpc://'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

def create_csv(data_list, csv_file_path):
    if not data_list:
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

@celery.task
def create_csv_barriers_task(data_json, output_folder):
    input_json = data_json
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    barrier_total = []
    for barrier in input_json:
        pointer_barrier = input_json[barrier].get("pointerBarrier", [])
        for cuenca in pointer_barrier:
            cuenca_in = {
                'Codigo Microcuenca': barrier,
                'Nombre': cuenca["name"],
                'Longitud': cuenca["lng"],
                'Latitud': cuenca["lat"],
            }
            barrier_total.append(cuenca_in)
    
    create_csv(barrier_total, os.path.join(output_folder, "Barriers.csv"))
    return data_json

@celery.task
def create_csv_WaterCatchments_task(data_json, output_folder):
    input_json = data_json
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    water_total = []
    for collection in input_json:
        pointer_collection = input_json[collection].get("pointerCollection", [])
        for water in pointer_collection:
            water_in = {
                'Codigo Microcuenca': collection,
                'Nombre': water["name"],
                'Longitud': water["lng"],
                'Latitud': water["lat"],
                'Caudal': water["cau"],
            }
            water_total.append(water_in)
    
    create_csv(water_total, os.path.join(output_folder, "WaterCatchments.csv"))
    return data_json

@celery.task
def create_csv_Pouring_task(data_json, output_folder):
    input_json = data_json
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    vertiment_total = []
    for pouring in input_json:
        pointer_pouring = input_json[pouring].get("pointerPouring", [])
        for vertiment in pointer_pouring:
            vertiment_in = {
                'Codigo Microcuenca': pouring,
                'Determinante': vertiment["name"],
                'Longitud': vertiment["lng"],
                'Latitud': vertiment["lat"],
                'Caudal (m3/s)': vertiment["cau"],
                'Solidos Suspendidos Totales (mg/l)': vertiment["data"]["sst"],
                'Coliformes Totales (NPM/100ml)': vertiment["data"]["coliformes_totales"],
                'Fosforo Organico (mg/l)': vertiment["data"]["fosforo_organico"],
                'Fosforo Inorganico (mg/l)': vertiment["data"]["fosforo_inorganico"],
                'Nitrogeno Organico (mg/l)': vertiment["data"]["nitrogeno_organico"],
                'Nitrogeno Amonical (mg/l)': vertiment["data"]["nitrogeno_amoniacal"],
                'Nitratos (mg/l)': vertiment["data"]["nitratos"],
                'DBO5 (mg/l)': vertiment["data"]["dbo5"],
                'Mercurio total (mg/l)': vertiment["data"]["mercurio_total"],       
            }
            vertiment_total.append(vertiment_in)
    
    create_csv(vertiment_total, os.path.join(output_folder, "Dumpings.csv"))
    return data_json

@celery.task
def create_csv_basin_task(data_json, output_folder):
    input_json = data_json
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

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
    
    create_csv(basin_total, os.path.join(output_folder, "BasinData.csv"))
    return data_json

@celery.task
def generate_csv_all(data_json, output_folder):
    task_chain = chain(
        create_csv_barriers_task.s(data_json, output_folder),
        create_csv_WaterCatchments_task.s(output_folder),
        create_csv_Pouring_task.s(output_folder),
        create_csv_basin_task.s(output_folder)
    )
    result = task_chain()
    return result

@app.route('/process_json', methods=['POST'])
def process_json():
    data = request.get_json()
    output_folder = "output_folder"  # Cambia esto según tu ruta de salida
    task = generate_csv_all.apply_async((data, output_folder))
    return jsonify({'task_id': task.id}), 202

@app.route('/resultado/<task_id>', methods=['GET'])
def resultado(task_id):
    task = celery.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'status': 'Pendiente...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'result': task.result
        }
    else:
        response = {
            'state': task.state,
            'status': str(task.info)
        }
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
