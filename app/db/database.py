import logging as log 
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import os 
from dotenv import load_dotenv

logger = log.getLogger(__name__)
load_dotenv()

user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
dbname = os.getenv("PG_DBNAME")

if not all([user, password, host, port, dbname]):
    logger.error("Variaveis não definidas no .env")
    raise ValueError("Variaveis não definidas")

try:
    URL_BANCO = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(URL_BANCO)
    SessionLocal = sessionmaker(autocommit= False, autoflush= False, bind= engine)
    logger.info("Conexão Ok ✅")
    
except Exception as e:
    logger.error(f"Erro na conexão ❌: {e}")
    raise 

