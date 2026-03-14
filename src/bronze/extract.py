import pandas as pd 
import logging as log 
import os 
from dotenv import load_dotenv

logger = log.getLogger(__name__)

def extract () -> pd.DataFrame:
    load_dotenv()
    logger.info("Iniciando extração")
    
    caminho_arquivo = os.getenv("CAMINHO_ARQUIVO")
    
    if not caminho_arquivo:
        logger.error("Caminho não definido no arquivo .env")
        raise ValueError("Variavel não definida no .env")
    
    df = pd.read_csv(caminho_arquivo, low_memory= False, encoding='utf-8', delimiter=";")
    logger.info("Arquivo extraido com sucesso / Colunas: %s, Linhas: %s", df.shape[1], len(df))
    return df 