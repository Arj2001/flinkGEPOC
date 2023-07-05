# import tensorflow as tf
from flask import Flask, request
import time

app = Flask(__name__)


@app.route('/', methods=['POST'])
def value_from_flink():
    consume_time = time.time() * 1000
    values = request.get_json()
    
    print(values)
    start_time = time.time()
    while (time.time() - start_time) < 0.15:
        pass
    print("total consumed time: ", time.time() * 1000 - consume_time)
    return str("value returned")


if __name__ == '__main__':
    from waitress import serve
    serve(app, host="0.0.0.0", port=5000)
