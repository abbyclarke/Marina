from flask import Blueprint, request, Response
from google.cloud import datastore
import json
import constants
from auth import verify_jwt

client = datastore.Client()

bp = Blueprint('boat', __name__, url_prefix='/boats')


@bp.route('', methods=['POST','GET', 'PUT', 'PATCH', 'DELETE'])
def boats_get_post():
    if request.method == 'POST':
        # if request is not JSON, 415 status returned
        if 'application/json' not in request.content_type:
            not_json = {"Error" : "Request must be content type application/json"}
            return Response(json.dumps(not_json), status= 415)
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        # verify user
        payload = verify_jwt(request)
        owner = payload["sub"]
        content = request.get_json()
        new_boat = datastore.entity.Entity(key=client.key(constants.boats))
        # if missing required attribute, 400 status code returned
        if len(content.items()) < 3:
            bad_request = {"Error" : "The request object is missing at least one of the required attributes"}
            return Response(json.dumps(bad_request), status= 400)
        new_boat.update({'name': content['name'], 'type': content['type'],
          'length': content['length'], "owner": owner, 'loads': []})
        client.put(new_boat)
        new_boat["id"] = new_boat.key.id
        new_boat["self"] = request.url_root + "boats/" + str(new_boat["id"])
        return Response(json.dumps(new_boat), status= 201)
    elif request.method == 'GET':
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        # verify user and show user's boats 5 per page
        payload = verify_jwt(request)
        owner = payload["sub"]
        query_count = client.query(kind=constants.boats)
        query_count.add_filter("owner", "=", owner)
        total_items = len(list(query_count.fetch()))
        query = client.query(kind=constants.boats)
        query.add_filter("owner", "=", owner)
        q_limit = int(request.args.get('limit', '5'))
        q_offset = int(request.args.get('offset', '0'))
        l_iterator = query.fetch(limit= q_limit, offset=q_offset)
        pages = l_iterator.pages
        results = list(next(pages))
        if l_iterator.next_page_token:
            next_offset = q_offset + q_limit
            next_url = request.base_url + "?limit=" + str(q_limit) + "&offset=" + str(next_offset)
        else:
            next_url = None
        for e in results:
            e["id"] = e.key.id
            e["self"] = request.url_root + "boats/" + str(e["id"])
            for load in e["loads"]:
                if load:
                    load["self"] = request.url_root + "loads/" + str(load["id"])
        output = {"total_items": total_items, "boats": results}
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

@bp.route('/<id>', methods=['PUT', 'PATCH', 'DELETE', 'GET'])
def boats_put_delete(id):
    if request.method == 'PUT':
        # if request is not JSON, 415 status returned
        if 'application/json' not in request.content_type:
            not_json = {"Error" : "Request must be content type application/json"}
            return Response(json.dumps(not_json), status= 415)
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        # verify user
        payload = verify_jwt(request)
        owner = payload["sub"]
        content = request.get_json()
        boat_key = client.key(constants.boats, int(id))
        boat = client.get(key=boat_key)
        # if invalid id, 404 status code
        if not boat:
            not_found = {"Error" : "No boat with this boat_id exists"}
            return Response(json.dumps(not_found), status= 404)
        if boat["owner"] != owner:
            wrong_owner = {"Error" : "The boat with this id has a different owner"}
            return Response(json.dumps(wrong_owner), status= 403)
        # if missing required attribute, 400 status code returned
        if len(content.items()) < 3:
            bad_request = {"Error" : "The request object is missing at least one of the required attributes"}
            return Response(json.dumps(bad_request), status= 400)
        boat.update({'name': content['name'], 'type': content['type'],
          'length': content['length']})
        client.put(boat)
        boat["id"] = boat.key.id
        boat["self"] = request.url_root + "boats/" + str(boat["id"])
        return Response(json.dumps(boat), status= 200)
    elif request.method == 'PATCH':
        # if request is not JSON, 415 status returned
        if 'application/json' not in request.content_type:
            not_json = {"Error" : "Request must be content type application/json"}
            return Response(json.dumps(not_json), status= 415)
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        # verify user
        payload = verify_jwt(request)
        owner = payload["sub"]
        content = request.get_json()
        boat_key = client.key(constants.boats, int(id))
        boat = client.get(key=boat_key)
        # if invalid id, 404 status code
        if not boat:
            not_found = {"Error" : "No boat with this boat_id exists"}
            return Response(json.dumps(not_found), status= 404)
        if boat["owner"] != owner:
            wrong_owner = {"Error" : "The boat with this id has a different owner"}
            return Response(json.dumps(wrong_owner), status= 403)
        # if missing required attribute, 400 status code returned
        if len(content.items()) < 1:
            bad_request = {"Error" : "The request object is empty"}
            return Response(json.dumps(bad_request), status= 400)
        boat.update(content)
        client.put(boat)
        boat["id"] = boat.key.id
        boat["self"] = request.url_root + "boats/" + str(boat["id"])
        return Response(json.dumps(boat), status= 200)
    elif request.method == 'DELETE':
        # verify user
        payload = verify_jwt(request)
        owner = payload["sub"]
        key = client.key(constants.boats, int(id))
        boat = client.get(key=key)
        if not boat:
            not_found = {"Error" : "No boat with this boat_id exists"}
            return Response(json.dumps(not_found), status= 404)
        if boat["owner"] != owner:
            wrong_owner = {"Error" : "The boat with this id has a different owner"}
            return Response(json.dumps(wrong_owner), status= 403)
        # check if boat has loads, update loads' carrier before deleting boat
        for l in boat["loads"]:
            load_id = l["id"]
            load_key = client.key(constants.loads, int(load_id))
            load = client.get(key=load_key)
            load["carrier"] = None
            client.put(load)
        client.delete(key)
        return ('',204)
    elif request.method == 'GET':
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        # verify user
        payload = verify_jwt(request)
        owner = payload["sub"]
        boat_key = client.key(constants.boats, int(id))
        boat = client.get(key=boat_key)
        # if invalid id, 404 status code
        if not boat:
            not_found = {"Error" : "No boat with this boat_id exists"}
            return Response(json.dumps(not_found), status= 404)
        if boat["owner"] != owner:
            wrong_owner = {"Error" : "The boat with this id has a different owner"}
            return Response(json.dumps(wrong_owner), status= 403)
        boat["id"] = boat.key.id
        boat["self"] = request.url_root + "boats/" + str(boat["id"])
        for load in boat["loads"]:
            if load:
                load["self"] = request.url_root + "loads/" + str(load["id"])
        return Response(json.dumps(boat), status= 200)
    else:
        return 'Method not recognized'

@bp.route('/<bid>/loads/<lid>', methods=['PUT','DELETE'])
def add_delete_load(bid,lid):
    if request.method == 'PUT':
        # verify user
        payload = verify_jwt(request)
        owner = payload["sub"]
        boat_key = client.key(constants.boats, int(bid))
        boat = client.get(key=boat_key)
        load_key = client.key(constants.loads, int(lid))
        load = client.get(key=load_key)
        if not load or not boat:
            not_found = {"Error" : "The specified boat and/or load does not exist"}
            return Response(json.dumps(not_found), status= 404)
        if load["carrier"]:
            already_loaded = {"Error" : "The load is already loaded on another boat"}
            return Response(json.dumps(already_loaded), status= 403)
        if boat["owner"] != owner:
            wrong_owner = {"Error" : "The boat with this id has a different owner"}
            return Response(json.dumps(wrong_owner), status= 403)
        # add load to boat's load list
        if 'loads' in boat.keys():
            boat['loads'].append({"id": load.id})
        else:
            boat['loads'] = [{"id": load.id}]
        # add specified boat to load
        load["carrier"] = {"id": boat.id, "name": boat["name"]}
        client.put_multi([boat, load])
        return('',204)
    elif request.method == 'DELETE':
        # verify user
        payload = verify_jwt(request)
        owner = payload["sub"]
        boat_key = client.key(constants.boats, int(bid))
        boat = client.get(key=boat_key)
        load_key = client.key(constants.loads, int(lid))
        load = client.get(key=load_key)
        if not load or not boat:
            not_found = {"Error" : "No boat with this boat_id is loaded with the load with this load_id"}
            return Response(json.dumps(not_found), status= 404)
        if boat["owner"] != owner:
            wrong_owner = {"Error" : "The boat with this id has a different owner"}
            return Response(json.dumps(wrong_owner), status= 403)
        load_present = False
        for l in boat["loads"]:
            if l and l["id"] == load.id:
                load_present = True
                break
        if not load_present:
            not_present = {"Error" : "No boat with this boat_id is loaded with the load with this load_id"}
            return Response(json.dumps(not_present), status= 404)
        # remove load from boat's load list
        for l in boat["loads"]:
            if l and l["id"] == load.id:
                boat['loads'].remove(l)
        # remove boat from load's specified carrier
        load["carrier"] = None
        client.put_multi([boat, load])
        return('',204)
    else:
        return 'Method not recognized'

@bp.route('/<id>/loads', methods=['GET'])
def get_loads(id):
    if request.method == 'GET':
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        # verify user
        payload = verify_jwt(request)
        owner = payload["sub"]
        boat_key = client.key(constants.boats, int(id))
        boat = client.get(key=boat_key)
        if not boat:
            not_found = {"Error" : "No boat with this boat_id exists"}
            return Response(json.dumps(not_found), status= 404)
        if boat["owner"] != owner:
            wrong_owner = {"Error" : "The boat with this id has a different owner"}
            return Response(json.dumps(wrong_owner), status= 403)
        query = client.query(kind=constants.loads)
        query.add_filter("carrier.id", "=", int(id))
        q_limit = int(request.args.get('limit', '5'))
        q_offset = int(request.args.get('offset', '0'))
        l_iterator = query.fetch(limit= q_limit, offset=q_offset)
        pages = l_iterator.pages
        results = list(next(pages))
        if l_iterator.next_page_token:
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
        output = {"loads": results}
        if next_url:
            output["next"] = next_url
        return Response(json.dumps(output), status= 200)
    else:
        return 'Method not recognized'
    
