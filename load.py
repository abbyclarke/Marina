from flask import Blueprint, request, Response
from google.cloud import datastore
import json
import constants

client = datastore.Client()

bp = Blueprint('load', __name__, url_prefix='/loads')

@bp.route('', methods=['POST','GET', 'PUT', 'PATCH', 'DELETE'])
def loads_get_post():
    if request.method == 'POST':
        # if request is not JSON, 415 status returned
        if 'application/json' not in request.content_type:
            not_json = {"Error" : "Request must be content type application/json"}
            return Response(json.dumps(not_json), status= 415)
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        content = request.get_json()
        new_load = datastore.entity.Entity(key=client.key(constants.loads))
        # if missing required attribute, 400 status code returned
        if len(content.items()) < 3:
            bad_request = {"Error" : "The request object is missing at least one of the required attributes"}
            return Response(json.dumps(bad_request), status= 400)
        new_load.update({"volume": content["volume"], "item": content["item"], "creation_date": content["creation_date"], "carrier": None})
        client.put(new_load)
        new_load["id"] = new_load.key.id
        new_load["self"] = request.url_root + "loads/" + str(new_load["id"])
        return Response(json.dumps(new_load), status= 201)
    elif request.method == 'GET':
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        query_count = client.query(kind=constants.loads)
        total_items = len(list(query_count.fetch()))
        query = client.query(kind=constants.loads)
        q_limit = int(request.args.get('limit', '5'))
        q_offset = int(request.args.get('offset', '0'))
        g_iterator = query.fetch(limit= q_limit, offset=q_offset)
        pages = g_iterator.pages
        results = list(next(pages))
        if g_iterator.next_page_token:
            next_offset = q_offset + q_limit
            next_url = request.base_url + "?limit=" + str(q_limit) + "&offset=" + str(next_offset)
        else:
            next_url = None
        for e in results:
            e["id"] = e.key.id
            e["self"] = request.url_root + "loads/" + str(e["id"])
            carrier = e["carrier"]
            if carrier:
                carrier["self"] = request.url_root + "boats/" + str(carrier["id"])
        output = {"total_items": total_items, "loads": results}
        if next_url:
            output["next"] = next_url
        return Response(json.dumps(output), status= 200)
    elif request.method == 'PUT':
        unsupported_method = {"Error" : "Method not supported"}
        return Response(json.dumps(unsupported_method), status= 405)
    elif request.method == 'PATCH':
        unsupported_method = {"Error" : "Method not supported"}
        return Response(json.dumps(unsupported_method), status= 405)
    elif request.method == 'DELETE':
        unsupported_method = {"Error" : "Method not supported"}
        return Response(json.dumps(unsupported_method), status= 405)
    else:
        return 'Method not recognized'


@bp.route('/<id>', methods=['PUT', 'DELETE', 'GET', 'PATCH'])
def loads_put_delete(id):
    if request.method == 'PUT':
        # if request is not JSON, 415 status returned
        if 'application/json' not in request.content_type:
            not_json = {"Error" : "Request must be content type application/json"}
            return Response(json.dumps(not_json), status= 415)
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        content = request.get_json()
        load_key = client.key(constants.loads, int(id))
        load = client.get(key=load_key)
        # if invalid id, 404 status code
        if not load:
            not_found = {"Error" : "No load with this load_id exists"}
            return Response(json.dumps(not_found), status= 404)
        # if missing required attributes, 400 status code returned
        if len(content.items()) < 3:
            bad_request = {"Error" : "The request object is missing at least one of the required attributes"}
            return Response(json.dumps(bad_request), status= 400)
        load.update({"volume": content["volume"], "item": content["item"], "creation_date": content["creation_date"]})
        client.put(load)
        load["id"] = load.key.id
        load["self"] = request.url_root + "loads/" + str(load["id"])
        return Response(json.dumps(load), status= 200)
    elif request.method == 'PATCH':
        # if request is not JSON, 415 status returned
        if 'application/json' not in request.content_type:
            not_json = {"Error" : "Request must be content type application/json"}
            return Response(json.dumps(not_json), status= 415)
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        content = request.get_json()
        load_key = client.key(constants.loads, int(id))
        load = client.get(key=load_key)
        # if invalid id, 404 status code
        if not load:
            not_found = {"Error" : "No load with this load_id exists"}
            return Response(json.dumps(not_found), status= 404)
        # if missing required attribute, 400 status code returned
        if len(content.items()) < 1:
            bad_request = {"Error" : "The request object is empty"}
            return Response(json.dumps(bad_request), status= 400)
        load.update(content)
        client.put(load)
        load["id"] = load.key.id
        load["self"] = request.url_root + "loads/" + str(load["id"])
        return Response(json.dumps(load), status= 200)
    elif request.method == 'DELETE':
        key = client.key(constants.loads, int(id))
        load = client.get(key=key)
        if not load:
            not_found = {"Error" : "No load with this load_id exists"}
            return Response(json.dumps(not_found), status= 404)
        # check if load has a carrier, remove load from boat
        if load["carrier"]:
            carrier = load["carrier"]
            boat_id = carrier["id"]
            boat_key = client.key(constants.boats, int(boat_id))
            boat = client.get(key=boat_key)
            for l in boat["loads"]:
                if l["id"] == int(id):
                    boat["loads"].remove(l)
                    client.put(boat)
        client.delete(key)
        return ('',204)
    elif request.method == 'GET':
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        load_key = client.key(constants.loads, int(id))
        load = client.get(key=load_key)
        # if invalid id, 404 status code
        if not load:
            not_found = {"Error" : "No load with this load_id exists"}
            return Response(json.dumps(not_found), status= 404)
        load["id"] = load.key.id
        load["self"] = request.url_root + "loads/" + str(load["id"])
        carrier = load["carrier"]
        if carrier:
            carrier["self"] = request.url_root + "boats/" + str(carrier["id"])
        return Response(json.dumps(load), status= 200)
    else:
        return 'Method not recogonized'