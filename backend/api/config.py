import os
from dotenv import load_dotenv

load_dotenv('.env')

class ClientConfig:
    # client isolation
    
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL")
    
    # Vector DB
    QDRANT_URL = os.getenv("QDRANT_URL")
    QDRANT_API_KEY = os.getenv("VECTORDB_KEY")
    COLLECTION_NAME = os.getenv("COLLECTION_NAME")
    
    # S3
    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_REGION = os.getenv("S3_REGION")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    # Redis
    REDIS_URL = os.getenv("REDIS_URL")
    
    # OpenAI
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    
    # keys mapping
    API_KEY_MAP = {
        # test client
        os.getenv("API_KEY_TEST"): os.getenv("DB_KEY")
    }
    
    @classmethod
    def get_client_id_from_api_key(cls, api_key: str) -> str:
        """Get client_id from API key"""
        return cls.API_KEY_MAP.get(api_key)
    
    @classmethod
    def validate(cls):
        """Validate all required configs are set"""
        required = [
            cls.DATABASE_URL,
            cls.QDRANT_URL,
            cls.QDRANT_API_KEY,
            cls.REDIS_URL,
            cls.OPENAI_API_KEY,
            cls.AWS_ACCESS_KEY_ID,
            cls.AWS_SECRET_ACCESS_KEY
        ]
        if not all(required):
            raise ValueError("Missing required environment variables")