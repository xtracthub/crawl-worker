
from globus_sdk import ConfidentialAppAuthClient
import time
import os


def get_uid_from_token(auth_token):
    # Step 1: Get Auth Client with Secrets.
    client_id = os.getenv("GLOBUS_FUNCX_CLIENT")
    secret = os.getenv("GLOBUS_FUNCX_SECRET")

    # Step 2: Transform token and introspect it.
    t0 = time.time()

    conf_app_client = ConfidentialAppAuthClient(client_id, secret)
    token = str.replace(str(auth_token), 'Bearer ', '')

    auth_detail = conf_app_client.oauth2_token_introspect(token)
    t1 = time.time()

    try:
        uid = auth_detail['username']
    except KeyError as e:
        raise ValueError("Unable to identify Globus user. Returning (to reject!)")

    print(f"Authenticated user {uid} in {t1-t0} seconds")

    # print(uid)
    return uid
