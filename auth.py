import os
from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from dotenv import load_dotenv

load_dotenv()  # ← carga .env para leer API_KEY

API_KEY = os.getenv("API_KEY")
api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)

def require_api_key(api_key: str = Security(api_key_header)):
    if not api_key or api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True
