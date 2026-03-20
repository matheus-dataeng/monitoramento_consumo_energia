import pandas as pd 
import logging as log 
from sqlalchemy import create_engine, text 
import os 
from dotenv import load_dotenv

logger = log.getLogger(__name__)

def load(
    dim_tempo: pd.DataFrame,
    dim_subsistema: pd.DataFrame,
    fato_energia: pd.DataFrame,

) -> pd.DataFrame:
    
    logger.info("Iniciando Load")
    load_dotenv()
    
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    dbname = os.getenv("PG_DBNAME")
    
    if not all ([user, password, host, port, dbname]):
        logger.error("Variaveis não definidas no .env")
        raise ValueError("Variveis não indentificadas")
    
    try:
        URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(URL)
        
        table_dim_tempo = os.getenv("TABLE_DIM_TEMPO") 
        table_dim_subsistema = os.getenv("TABLE_DIM_SUBSISTEMA")
        table_fato_energia = os.getenv("TABLE_FATO")
        
        if not all([table_dim_tempo, table_dim_subsistema, table_fato_energia]):
            logger.error("Variaveis das tabelas não definidas no .env")
            raise ValueError("Tabelas não definidas no .env")
        
        id_fato_carga = fato_energia["Id_tempo"].dropna().astype(int).unique().tolist()
        id_dim_tempo = dim_tempo["Id_tempo"].dropna().astype(int).unique().tolist()
        id_dim_subsistema = dim_subsistema["Id_subsistema"].dropna().astype(int).unique().tolist()
        
        with engine.begin() as conn:
            
            tables = [
                (table_fato_energia, "Id_tempo", id_fato_carga),
                (table_dim_tempo, "Id_tempo", id_dim_tempo),
                (table_dim_subsistema, "Id_subsistema", id_dim_subsistema)
            ]

            for tabelas, colunas, ids in tables:
                result = conn.execute(
                    text(f'DELETE FROM {tabelas} WHERE "{colunas}" = ANY(:id)'),
                    {"id": ids}
                )
                
                logger.info("Registros removidos da tabela %s: %s", tabelas, result.rowcount)
                
            dfs = [
                (dim_tempo, table_dim_tempo),
                (dim_subsistema, table_dim_subsistema),
                (fato_energia, table_fato_energia)
            ]

            for df, tabela in dfs:
                df.to_sql(name = tabela, con = conn, index = False, if_exists = "append", chunksize = 1000)
                logger.info("Tabela %s carregada / Colunas: %s, Linhas: %s", tabela, df.shape[1], len(df))
        
    except Exception as e:
        logger.error(f"Falha no Load: {e}")
        raise 
                
            
