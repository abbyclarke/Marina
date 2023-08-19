from flask import Flask, request, Response, jsonify, redirect, session, render_template, url_for
import boat
import load
from google.cloud import datastore
import json
import uuid
import json
from dotenv import load_dotenv
import os
from authlib.integrations.flask_client import OAuth
from urllib.parse import urlencode, quote_plus
from auth import verify_jwt, AuthError

app = Flask(__name__)
app.secret_key = str(uuid.uuid4())
app.register_blueprint(boat.bp)
app.register_blueprint(load.bp)
client = datastore.Client()
users = "users"
load_dotenv()
CLIENT_ID = os.getenv("client_id")
CLIENT_SECRET = os.getenv("client_secret")
DOMAIN = 'cs493-clarkeab.us.auth0.com'

ALGORITHMS = ["RS256"]

oauth = OAuth(app)

auth0 = oauth.register(
    'auth0',
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    api_base_url="https://" + DOMAIN,
    access_token_url="https://" + DOMAIN + "/oauth/token",
    authorize_url="https://" + DOMAIN + "/authorize",
    client_kwargs={
        'scope': 'openid profile email',
    },
    server_metadata_url=f'https://{DOMAIN}/.well-known/openid-configuration'
)

@app.errorhandler(AuthError)
def handle_auth_error(ex):
    response = jsonify(ex.error)
    response.status_code = ex.status_code
    return response

# auth0 login/callback/logout code adapted from https://auth0.com/docs/quickstart/webapp/python
@app.route('/')
def index():
    return render_template("home.html", session=session.get("user"), pretty=json.dumps(session.get("user"), indent=4))

@app.route('/callback', methods=["GET", "POST"])
def callback():
    token = oauth.auth0.authorize_access_token()
    session["user"] = token
    userinfo = token.get("userinfo")
    sub = userinfo.get("sub")
    name = userinfo.get("name")
    # check if user has been added to database
    query = client.query(kind=users)
    query.add_filter("sub", "=", sub)
    results = list(query.fetch())
    # add new user
    if len(results) < 1:
        new_user = datastore.entity.Entity(key=client.key(users))
        new_user.update({"sub": sub, "name": name})
        client.put(new_user)
    return redirect("/")

@app.route('/logout')
def logout():
    session.clear()
    return redirect(
        "https://"
        + DOMAIN
        + "/v2/logout?"
        + urlencode(
            {
                "returnTo": url_for("index", _external=True),
                "client_id": CLIENT_ID,
            },
            quote_via=quote_plus,
        )
    )

@app.route('/login')
def login():
    return oauth.auth0.authorize_redirect(redirect_uri=url_for("callback", _external=True))

@app.route('/users', methods=["GET"])
def get_users():
    if request.method == "GET":
        # if client does not accept response as JSON, 406 status returned
        if 'application/json' not in request.accept_mimetypes:
            not_json = {"Error" : "Accept header must accept response content type application/json"}
            return Response(json.dumps(not_json), status= 406)
        query = client.query(kind=users)
        results = list(query.fetch())
        for e in results:
            e["id"] = e.key.id
        return Response(json.dumps(results), status= 200)
    else:
        return jsonify(error='Method not recognized')

# Decode the JWT supplied in the Authorization header
@app.route('/decode', methods=['GET'])
def decode_jwt():
    payload = verify_jwt(request)
    return payload 


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)