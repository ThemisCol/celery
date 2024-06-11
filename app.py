from flask import Flask, request, jsonify
from celery import Celery

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'amqp://guest@rabbitmq//'
app.config['CELERY_RESULT_BACKEND'] = 'rpc://'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

@celery.task
def suma(x, y):
    return x + y

@app.route('/sumar', methods=['POST'])
def sumar():
    data = request.get_json()
    x = data['x']
    y = data['y']
    task = suma.apply_async((x, y))
    return jsonify({'task_id': task.id}), 202

@app.route('/resultado/<task_id>', methods=['GET'])
def resultado(task_id):
    task = suma.AsyncResult(task_id)
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
