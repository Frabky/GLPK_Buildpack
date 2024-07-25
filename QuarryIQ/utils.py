from cryptography.fernet import Fernet
import base64
import environ
from QuarryIQ.models import UploadedGLPK

# Initialize environment variables
env = environ.Env()
environ.Env.read_env()  # Ensure this is called to read from the .env file

# Get the encryption key from environment variables
ENCRYPTION_KEY = env('ENCRYPTION_KEY')
if not ENCRYPTION_KEY:
    raise ValueError("ENCRYPTION_KEY not found in .env file")

# Ensure the key is in bytes and base64 encoded
ENCRYPTION_KEY = base64.urlsafe_b64decode(ENCRYPTION_KEY)

# Initialize the Fernet cipher
cipher = Fernet(ENCRYPTION_KEY)

def encrypt_data(data):
    """Encrypt the data"""
    return cipher.encrypt(data.encode('utf-8'))

def decrypt_data(encrypted_data):
    """Decrypt the data"""
    return cipher.decrypt(encrypted_data).decode('utf-8')


def get_mod_file_content(mod_file):
    """Retrieve model, decrypt it, and return the content"""
    try:
        # Retrieve the latest B4C MinVC Base record from the database
        record = UploadedGLPK.objects.filter(original_file_name=mod_file).order_by('-upload_date').first()

        if not record:
            raise ValueError("No model record found in the database")

        # Decrypt the file content
        decrypted_content = decrypt_data(record.encrypted_content)
        return decrypted_content

    except Exception as e:
        print(f"An error occurred: {e}")
        return None