
from globus_sdk import ConfidentialAppAuthClient
import time
import os


def get_uid_from_token(auth_token):
    # Step 1: Get Auth Client with Secrets.
    client_id = os.getenv("globus_client")
    secret = os.getenv("globus_secret")

    # Step 2: Transform token and introspect it.
    t0 = time.time()

    conf_app_client = ConfidentialAppAuthClient(client_id, secret)
    token = str.replace(str(auth_token), 'Bearer ', '')

    auth_detail = conf_app_client.oauth2_token_introspect(token)
    t1 = time.time()

    try:
        print(auth_detail)
        uid = auth_detail['username']
    except KeyError as e:
        raise ValueError("Unable to identify Globus user. Returning (to reject!)")

    print(f"Authenticated user {uid} in {t1-t0} seconds")

    # print(uid)
    return uid

# tokens = {'crawl_id': '293e469e-30bc-4243-b09a-06f549c58222', 'transfer_token': 'Ag80Ewz79jy8oe29m4QrqYmlJnjnXljjYYBdvll9qGnQGa8K4Ys8CBqbKoWnYvQ7la9kqQ7G43j9MBh7Q0KPlty33z', 'auth_token': 'Ag80Ewz79jy8oe29m4QrqYmlJnjnXljjYYBdvll9qGnQGa8K4Ys8CBqbKoWnYvQ7la9kqQ7G43j9MBh7Q0KPlty33z'}
# get_uid_from_token('AgaDEQrr9kbogWWyJwJ8dM1GYw0lzvxk6dvYKGDkMKD71VOGO5u9CgxbxqyGQj333zlQaeKPbQNX1dTPJ2GvOuywwJ')