import configparser
import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


def derive_key(password, salt=b'salt', iterations=100000):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=iterations,
        backend=default_backend()
    )
    key = kdf.derive(password.encode())
    return base64.urlsafe_b64encode(key)


def encrypt_value(value, password):
    salt = os.urandom(16)
    key = derive_key(password, salt)
    cipher_suite = Fernet(key)
    encrypted_value = cipher_suite.encrypt(value.encode())
    return salt + encrypted_value


def decrypt_value(encrypted_value, password):
    salt = encrypted_value[:16]
    encrypted_data = encrypted_value[16:]
    key = derive_key(password, salt)
    cipher_suite = Fernet(key)
    decrypted_value = cipher_suite.decrypt(encrypted_data)
    return decrypted_value.decode()


def save_to_config(config_file_path, api_token, vault_path, password):
    encrypted_token = encrypt_value(api_token, password)
    encrypted_vault_path = encrypt_value(vault_path, password)
    config = configparser.ConfigParser()
    config['DEFAULT'] = {
        'API': encrypted_token.hex(),
        'VAULT': encrypted_vault_path.hex()
    }
    with open(config_file_path, 'w') as configfile:
        config.write(configfile)


def init_repo():
    config = configparser.ConfigParser()
    config['REPO_SETTINGS'] = {
        'data_folder': 'src/data/'
    }
    with open('settings.ini', 'w') as configfile:
        config.write(configfile)


def check_file(config_file_path):
    return os.path.exists(config_file_path)


def check_integrity():

    config_file_path = 'config.ini'

    if not check_file(config_file_path):

        todoist_token = input("Enter your Todoist token: ")
        vault_path = input("Enter the absolute path to your Obsidian vault: ")
        password = input("Enter a password to encrypt your data: ")

        save_to_config(config_file_path, todoist_token, vault_path, password)
        init_repo()

        print("Config file created successfully with encrypted values.")
        
        return None
    
    else:

        config = configparser.ConfigParser()
        config.read(config_file_path)
        api_token_encrypted = bytes.fromhex(config['DEFAULT']['API'])
        vault_path_encrypted = bytes.fromhex(config['DEFAULT']['VAULT'])
        password = input("Enter the password to decrypt your data: ")

        api_token_decrypted = decrypt_value(api_token_encrypted, password)
        vault_path_decrypted = decrypt_value(vault_path_encrypted, password)

        return {'token': api_token_decrypted,'vault_path': vault_path_decrypted}
    

if __name__ == '__main__':
    init_repo()
    
