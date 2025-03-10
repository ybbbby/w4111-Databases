####################################################################################################
#
# DO NOT WORRY ABOUT ANY OF THE STUFF IN THIS SECTION. THIS HELPS YOU IMPLEMENT.
#
#


# Import functions and objects the microservice needs.
# - Flask is the top-level application. You implement the application by adding methods to it.
# - Response enables creating well-formed HTTP/REST responses.
# - requests enables accessing the elements of an incoming HTTP/REST request.
#
from flask import Flask, Response, request
from datetime import datetime
import json
import src.data_service.data_table_adaptor as dta
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# The convention is that a compound primary key in a path has the elements sepatayed by "_"
# For example, /batting/willite01_BOS_1960_1 maps to the primary key for batting
_key_delimiter = "_"
_host = "127.0.0.1"
_port = 8000
_api_base = "/api"
application = Flask(__name__)


def handle_args(args):
    """

    :param args: The dictionary form of request.args.
    :return: The values removed from lists if they are in a list. This is flask weirdness.
        Sometimes x=y gets represented as {'x': ['y']} and this converts to {'x': 'y'}
    """

    result = {}

    if args is not None:
        for k, v in args.items():
            if type(v) == list:
                v = v[0]
            result[k] = v

    return result


# 1. Extract the input information from the requests object.
# 2. Log the information
# 3. Return extracted information.
#
def log_and_extract_input(method, path_params=None):
    path = request.path
    args = dict(request.args)
    data = None
    headers = dict(request.headers)
    method = request.method
    url = request.url
    base_url = request.base_url

    try:
        if request.data is not None:
            data = request.json
        else:
            data = None
    except Exception as e:
        # This would fail the request in a more real solution.
        data = "You sent something but I could not get JSON out of it."

    log_message = str(datetime.now()) + ": Method " + method

    # Get rid of the weird way that Flask sometimes handles query parameters.
    args = handle_args(args)

    inputs = {
        "path": path,
        "method": method,
        "path_params": path_params,
        "query_params": args,
        "headers": headers,
        "body": data,
        "url": url,
        "base_url": base_url
    }

    # Pull out the fields list as a separate element.
    if args and args.get('limit', None):
        limit = int(args.get('limit'))
        del args['limit']
        inputs['links'] = []
        if args.get('offset', None):
            offset = int(args.get('offset'))
            del args['offset']
        else:
            offset = 0
        inputs['limit'] = str(limit)
        inputs['offset'] = str(offset)
        base_url += "?"
        for k, v in args.items():
            base_url += k + '=' + v
            base_url += '&'
        tem = base_url
        tem += 'offset' + '=' + str(offset) + '&'
        tem += 'limit' + '=' + str(limit)
        inputs['links'].append({"ref": "current", "href": tem})
        tem = base_url
        pp = offset + limit
        tem += 'offset' + '=' + str(pp) + '&'
        tem += 'limit' + '=' + str(limit)
        inputs['links'].append({"ref": "next", "href": tem})
        if offset - limit > 0 or offset != 0:
            if offset - limit > 0:
                o = offset - limit
            else:
                o = 0
            tem = base_url
            tem += 'offset' + '=' + str(o) + '&'
            tem += 'limit' + '=' + str(limit)
            inputs['links'].append({"ref": "previous", "href": tem})

    if args and args.get('fields', None):
        fields = args.get('fields')
        fields = fields.split(",")
        del args['fields']
        inputs['fields'] = fields

    log_message += " received: \n" + json.dumps(inputs, indent=2)
    logger.debug(log_message)

    return inputs


def log_response(path, rsp):
    """

    :param path: The path parameter received.
    :param rsp: Response object
    :return:
    """
    msg = rsp
    logger.debug(str(datetime.now()) + ": \n" + str(rsp))


def get_field_list(inputs):
    return inputs.get('fields', None)


def generate_error(status_code, ex=None, msg=None):
    """

    This used to be more complicated in previous semesters, but we simplified for fall 2019.
    Does not do much now.
    :param status_code:
    :param ex:
    :param msg:
    :return:
    """

    rsp = Response("Oops", status=500, content_type="text/plain")

    if status_code == 500:
        if msg is None:
            msg = "INTERNAL SERVER ERROR. Please take COMSE6156 -- Cloud Native Applications."

        rsp = Response(msg, status=status_code, content_type="text/plain")

    return rsp


####################################################################################################
#
# THESE ARE JUST SOME EXAMPLES TO HELP YOU UNDERSTAND WHAT IS GOING ON.
#
#

# This function performs a basic health check. We will flesh this out.
@application.route("/health", methods=["GET"])
def health_check():
    rsp_data = {"status": "healthy", "time": str(datetime.now())}
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="application/json")
    return rsp


@application.route("/demo/<parameter>", methods=["GET", "PUT", "DELETE", "POST"])
def demo(parameter):
    """
    This simple echoes the various elements that you get for handling a REST request.
    Look at https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data

    :param parameter: A list of the path parameters.
    :return: None
    """

    inputs = log_and_extract_input(demo, {"parameter": parameter})

    msg = {
        "/demo received the following inputs": inputs,
    }

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp


####################################################################################################
#
# YOU HAVE TO COMPLETE THE IMPLEMENTATION OF THE FUNCTIONS BELOW.
#
#
@application.route("/api/databases", methods=["GET"])
def dbs():
    """

    :return: A JSON object/list containing the databases at this endpoint.
    """
    # -- TO IMPLEMENT --

    # Your code  goes here.

    # Hint: Implement the function in data_table_adaptor
    #
    msg = dta.get_databases()
    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp


@application.route("/api/databases/<dbname>", methods=["GET"])
def tbls(dbname):
    """

    :param dbname: The name of a database/sche,a
    :return: List of tables in the database.
    """

    inputs = log_and_extract_input(dbs, None)

    # Your code  goes here.

    # Hint: Implement the function in data_table_adaptor
    #

    msg = dta.get_tables(dbname)
    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp


@application.route('/api/<dbname>/<resource>/<primary_key>', methods=['GET', 'PUT', 'DELETE'])
def resource_by_id(dbname, resource, primary_key):
    """

    :param dbname: Schema/database name.
    :param resource: Table name.
    :param primary_key: Primary key in the form "col1_col2_..._coln" with the values of key columns.
    :return: Result of operations.
    """

    result = None

    try:
        # Parse the incoming request into an application specific format.
        context = log_and_extract_input(resource_by_id, (dbname, resource, primary_key))

        #
        # SOME CODE GOES HERE
        #
        # -- TO IMPLEMENT --
        dta.get_rdb_table(resource, dbname)
        if request.method == 'GET':
            if context.get('limit', None):
                msg = dta.getDataByKey(resource, dbname, primary_key.split("_"), context.get('fields', None),
                                       context['limit'], context['offset'])
            else:
                msg = dta.getDataByKey(resource, dbname, primary_key.split("_"), context.get('fields', None))
            asd = json.dumps(msg, indent=4, sort_keys=True, default=str)
            rsp = Response(asd, status=200, content_type="application/json")

        elif request.method == 'DELETE':
            msg = dta.deleteDataByKey(resource, dbname, primary_key.split("_"))
            asd = json.dumps(msg, indent=4, sort_keys=True, default=str)
            rsp = Response("HTTP: 200 You successfully Delete" + asd + "row(s)", status=200,
                           content_type="application/json")

        elif request.method == 'PUT':
            msg = dta.updateDataByKey(resource, dbname, primary_key.split("_"), context['body'])
            asd = json.dumps(msg, indent=4, sort_keys=True, default=str)
            rsp = Response("HTTP: 200 You successfully Update" + asd + "row(s)", status=200,
                           content_type="application/json")
    except Exception as e:
        print(e)
        return handle_error(e, result)

    return rsp


@application.route('/api/<dbname>/<resource_name>', methods=['GET', 'POST'])
def get_resource(dbname, resource_name):
    result = None

    try:
        context = log_and_extract_input(get_resource, (dbname, resource_name))

        dta.get_rdb_table(resource_name, dbname)
        if request.method == 'GET':
            if context.get('limit', None):
                msg = dta.getDataByTem(resource_name, dbname, context["query_params"], context.get('fields', None),
                                       context['limit'], context['offset'])
                if len(msg) < int(context['limit']):
                    context['links'].remove(context['links'][1])
                if len(context['links']) > 1:
                    msg.append(context['links'])
            else:
                msg = dta.getDataByTem(resource_name, dbname, context["query_params"], context.get('fields', None))
            asd = json.dumps(msg, indent=4, sort_keys=True, default=str)
            rsp = Response(asd, status=200, content_type="application/json")

        elif request.method == 'POST':
            msg = dta.insert(resource_name, dbname, context["body"])
            asd = json.dumps(msg, indent=4, sort_keys=True, default=str)
            rsp = Response("HTTP: 200 Entry successfully Inserted", status=200, content_type="application/json")
        else:
            result = "Invalid request."
            return result, 400, {'Content-Type': 'text/plain; charset=utf-8'}
    except Exception as e:
        print("Exception e = ", e)
        return handle_error(e, result)
    return rsp


@application.route('/api/<dbname>/<parent_name>/<primary_key>/<target_name>', methods=['GET'])
def get_by_path(dbname, parent_name, primary_key, target_name):
    # Do not implement

    result = " -- THANK ALY AND ARA -- "

    return result, 501, {'Content-Type': 'application/json; charset=utf-8'}


@application.route('/api/<dbname>/<parent_name>/<primary_key>/<target_name>/<target_key>',
                   methods=['GET'])
def get_by_path_key(dbname, parent_name, primary_key, target_name, target_key):
    # Do not implement

    result = " -- THANK ALY AND ARA -- "

    return result, 501, {'Content-Type': 'application/json; charset=utf-8'}


# You can ignore this method.
def handle_error(e, result):
    return e, 504, {'Content-Type': 'text/plain; charset=utf-8'}


# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.

    logger.debug("Starting HW2 time: " + str(datetime.now()))
    application.debug = True
    application.run(host=_host, port=_port)
