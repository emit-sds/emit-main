"""
This code encrypts passwords stored in a json file to be used by the workflow manager.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import json
import os

from cryptography.fernet import Fernet


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("passwords_file", help="Path to plain text passwords file")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    # 1. read your password file
    with open(args.passwords_file) as f:
        passwords = json.load(f)

    # 2. generate key and write it in a file
    key = Fernet.generate_key()
    key_file = os.path.join(os.path.dirname(args.passwords_file), "key.txt")
    f = open(key_file, "wb")
    f.write(key)
    f.close()

    # 3. encrypt the password and write it in a file
    enc_passwords_file = os.path.join(os.path.dirname(args.passwords_file), "encrypted_passwords.json")
    enc_passwords = {}
    refKey = Fernet(key)
    for k, v in passwords.items():
        mypwdbyt = bytes(v, 'utf-8')  # convert into byte
        encryptedPWD = refKey.encrypt(mypwdbyt)
        enc_passwords[k] = encryptedPWD.decode("utf-8")

    with open(enc_passwords_file, "w") as f:
        json.dump(enc_passwords, f, indent=4)


if __name__ == '__main__':
    main()
