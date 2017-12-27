import logging
import uuid

import nexusproto.NexusContent_pb2 as nexusproto
from flask import Flask, request, jsonify, Response
from flask.json import JSONEncoder
from flask_accept import accept
from google.protobuf import json_format
from werkzeug.exceptions import HTTPException, BadRequest
from werkzeug.exceptions import default_exceptions

from processors.processorchain import ProcessorChain, ProcessorNotFound, MissingProcessorArguments

applog = logging.getLogger(__name__)
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

    input_data = parameters['input_data']

    result = next(chain.process(input_data), None)

    if isinstance(result, nexusproto.NexusTile):
        result = result.SerializeToString()

    return Response(result, mimetype='application/octet-stream')


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
    app.run()
