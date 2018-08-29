# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import pprint
import sys
import uuid

from flask import Flask, request, jsonify, Response
from flask.json import JSONEncoder
from flask_accept import accept
from google.protobuf import json_format
from google.protobuf.json_format import ParseError
from nexusproto import DataTile_pb2 as nexusproto
from werkzeug.exceptions import HTTPException, BadRequest
from werkzeug.exceptions import default_exceptions

from sdap.processors.processorchain import ProcessorChain, ProcessorNotFound, MissingProcessorArguments

logging.basicConfig(format="%(asctime)s  %(levelname)s %(process)d --- [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S", stream=sys.stdout)

applog = logging.getLogger(__name__)
applog.setLevel(logging.INFO)
app = Flask(__name__)


class ProtobufJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, nexusproto.NexusTile):
                json_obj = json_format.MessageToJson(obj)
                return json_obj
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


@app.route('/processorchain', methods=['POST'], )
@accept('application/octet-stream', '*/*')
def run_processor_chain():
    try:
        parameters = request.get_json()
    except Exception as e:
        raise BadRequest("Invalid JSON data") from e

    try:
        processor_list = parameters['processor_list']
    except (KeyError, TypeError):
        raise BadRequest(description="processor_list is required.")

    try:
        chain = ProcessorChain(processor_list)
    except ProcessorNotFound as e:
        raise BadRequest("Unknown processor requested: %s" % e.missing_processor) from e
    except MissingProcessorArguments as e:
        raise BadRequest(
            "%s missing required configuration options: %s" % (e.processor, e.missing_processor_args)) from e

    try:
        input_data = json_format.Parse(parameters['input_data'], nexusproto.NexusTile())
    except ParseError as e:
        raise BadRequest("input_data must be a NexusTile protobuf serialized as a string") from e

    result = next(chain.process(input_data), None)

    if isinstance(result, nexusproto.NexusTile):
        result = result.SerializeToString()

    return Response(result, mimetype='application/octet-stream')


@app.route('/healthcheck', methods=['GET'], )
def health_check():
    return ''


def handle_error(e):
    error_id = uuid.uuid4()

    app.logger.exception("Exception %s" % error_id)
    code = 500
    message = "Internal server error"
    if isinstance(e, HTTPException):
        code = e.code
        message = str(e)
    return jsonify(message=message, error_id=error_id), code


if __name__ == '__main__':

    app.register_error_handler(Exception, handle_error)
    for ex in default_exceptions:
        app.register_error_handler(ex, handle_error)
    app.json_encoder = ProtobufJSONEncoder
    app.config.from_object('sdap.default_settings')
    try:
        app.config.from_envvar('NINGESTERPY_SETTINGS')
    except RuntimeError:
        applog.warning("NINGESTERPY_SETTINGS environment variable not set or invalid. Using default settings.")

    # SERVER_NAME should be in the form of localhost:5000, 127.0.0.1:0, etc...
    # So, split on : and take the second element as the port
    # If the port is 0, we need to pick a random port and then tell the server to use that socket
    if app.config['SERVER_NAME'] and int(app.config['SERVER_NAME'].split(':')[1]) == 0:
        import socket, os

        # Chose a random available port by binding to port 0
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((app.config['SERVER_NAME'].split(':')[0], 0))
        sock.listen()

        # Tells the underlying WERKZEUG server to use the socket we just created
        os.environ['WERKZEUG_SERVER_FD'] = str(sock.fileno())

        # Update the configuration so it matches with the port we just chose
        # (sock.getsockname will return the actual port being used, not 0)
        app.config['SERVER_NAME'] = '%s:%d' % (sock.getsockname())

    # Optionally write the current port to a file on the filesystem
    if app.config['CREATE_PORT_FILE']:
        with open(app.config['PORT_FILE'], 'w') as port_file:
            port_file.write(app.config['SERVER_NAME'].split(':')[1])

    applog.info("Running app on %s" % (app.config['SERVER_NAME']))
    applog.info("Active Settings:%s" % pprint.pformat(app.config, compact=True))
    app.run()
