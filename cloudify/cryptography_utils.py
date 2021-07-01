#########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

import base64
import json
import os

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from cloudify.constants import SECURITY_FILE_LOCATION

# encryption-related functions work in terms of accepting and returning
# unicode. Returning encrypted data as unicode is not very "pure", but
# it is more useful to handle; fernet returns base64-encoded data, so
# it is always possible to decode it


def encrypt(data, key=None):
    """Encrypt the data using the given key.

    :param data: unicode string to be encrypted
    :param key: encryption key - 64 url-safe base64-encoded bytes; if
                not provided, use the restservice config key
    :return: ciphertext, as unicode string
    """
    if data is None:
        return None
    key = key or _get_encryption_key()
    fernet = Fernet256(key)
    return fernet.encrypt(data.encode('utf-8')).decode('utf-8')


def decrypt(encrypted_data, key=None):
    """Decrypt the ciphertext using the given key.

    :param encrypted_data: ciphertext, as unicode string
    :param key: encryption key - 64 url-safe base64-encoded bytes; if
                not provided, use the restservice config key
    :return: decrypted data, as unicode script
    """
    if encrypted_data is None:
        return None
    key = key or _get_encryption_key()
    try:
        fernet = Fernet256(key)
    except ValueError:
        return decrypt128(encrypted_data, key)
    return fernet.decrypt(encrypted_data.encode('utf-8')).decode('utf-8')


def decrypt128(encrypted_data, key=None):
    if encrypted_data is None:
        return None
    key = key or _get_encryption_key()
    fernet = Fernet(key)
    return fernet.decrypt(encrypted_data.encode('utf-8')).decode('utf-8')


def _get_encryption_key():
    # We should have used config.instance.security_encryption_key to get the
    # key, but in snapshot restore the encryption key get updated in the
    # config file (rest-security.conf) but not in the memory. This is a temp
    # solution until we will have dynamic configuration mechanism
    with open(SECURITY_FILE_LOCATION) as security_conf_file:
        rest_security_conf = json.load(security_conf_file)
    return rest_security_conf['encryption_key'].encode('utf-8')


def generate_key_using_password(password, salt=b'salt_'):
    password = password.encode()
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=64,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(password))
    return key


class Fernet256(Fernet):
    def __init__(self, key, **kwargs):
        super(Fernet256, self).__init__(Fernet.generate_key(), **kwargs)
        key = base64.urlsafe_b64decode(key)
        if len(key) != 64:
            raise ValueError(
                "Fernet256 key must be 64 url-safe base64-encoded bytes."
            )

        self._signing_key = key[:32]
        self._encryption_key = key[32:]

    @classmethod
    def generate_key(cls):
        return base64.urlsafe_b64encode(os.urandom(64))
