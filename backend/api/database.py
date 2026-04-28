# database.py
from sqlalchemy import create_engine, Column, Integer, String, Index
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from sqlalchemy import BigInteger
from sqlalchemy import String

# Database connection 
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")


# Create engine
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True,         
    pool_size=10,               
    max_overflow=20,            
    echo=False,                 
)

# Session maker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()

# Document metadata model
class DocumentMetadata(Base):
    __tablename__ = "documents"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    client_id = Column(String(36), nullable=False, index=True)  
    document_id = Column(String(36), nullable=False, index=True) 
    markdown_filename = Column(String(255), index=True) 
    title = Column(String(255))
    original_filename = Column(String(255))
    type = Column(String(100))
    original_s3_url = Column(String(512)) # for viewing pdf version
    parsed_s3_url = Column(String(512))
    download_s3_url = Column(String(512)) # to download the original documents
    date_updated = Column(String(50))
    total_pages = Column(Integer)
    file_size = Column(String(50))

    __table_args__ = (
        Index('ix_client_document', 'client_id', 'document_id', unique=True),
        Index('ix_client_markdown', 'client_id', 'markdown_filename', unique=True),
    )

# Create tables
def init_db():
    Base.metadata.create_all(bind=engine)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()