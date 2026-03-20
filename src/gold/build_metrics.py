import pandas as pd 
import logging as log 

logger = log.getLogger(__name__)

def dimensao_tempo(df: pd.DataFrame) -> pd.DataFrame:
    dim_tempo = df[[
        "Data",
        "Ano",
        "Mes",
        "Dia",
        "Trimestre",
        "Dia_semana"
    ]].drop_duplicates()
    
    dim_tempo["Id_tempo"] = dim_tempo.index +1
    
    dim_tempo = dim_tempo[[
        "Id_tempo",
        "Data",
        "Ano",
        "Mes",
        "Dia",
        "Trimestre",
        "Dia_semana"
    ]]
    
    return dim_tempo

def dimensao_subsistema(df: pd.DataFrame) -> pd.DataFrame:
    dim_subsistema = df[[
        "Sigla",
        "Subsistema"
    ]].drop_duplicates()
    
    dim_subsistema["Id_subsistema"] = dim_subsistema.index +1
    
    dim_subsistema = dim_subsistema[[
        "Id_subsistema",
        "Sigla",
        "Subsistema"   
    ]]

    return dim_subsistema

def fato_carga_energia(
    df: pd.DataFrame,
    dim_tempo: pd.DataFrame,
    dim_subsistema : pd.DataFrame
    
) -> pd.DataFrame: 
    fato_energia = df.merge(
        dim_tempo,
        on=["Data", "Ano", "Mes", "Dia", "Trimestre", "Dia_semana"],
        how= "left"
    )  
    
    fato_energia = fato_energia.merge(
        dim_subsistema,
        on=["Sigla", "Subsistema"],
        how= "left"
    )
    
    fato_energia = fato_energia[[
        "Id_tempo",
        "Id_subsistema",
        "Carga_energia",
        "Data"
    ]]
    
    return fato_energia

def build_metrics(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Iniciando construção da camada Gold ")
    
    logger.info("Criando dimensões")
    
    dim_tempo = dimensao_tempo(df)

    try:
        dim_tempo.to_parquet("data_lake/gold/dim_tempo.parquet", index= False)
        logger.info("Dimensão Tempo salva no Data Lake Gold")
        
    except Exception as e: 
        logger.error(f"Erro ao salvar Dimensão Tempo no Data Lake Gold: {e}")
    
    logger.info("Dimensão tempo criada / Colunas: %s, Linhas: %s", dim_tempo.shape[1], len(dim_tempo))

    dim_subsistema = dimensao_subsistema(df)

    try:
        dim_subsistema.to_parquet("data_lake/gold/dim_subsistema.parquet", index= False)
        logger.info("Dimensão Subsistema salva no Data Lake Gold")
        
    except Exception as e: 
        logger.error(f"Erro ao salvar Dimensão Subsistema no Data Lake Gold: {e}")
        
    logger.info("Dimensão subsistema criada / Colunas: %s, Linhas: %s", dim_subsistema.shape[1], len(dim_subsistema))

    logger.info("Criando fato_carga_energia")
    fato_energia = fato_carga_energia(
        df,
        dim_tempo,
        dim_subsistema
    )
    
    try:
        fato_energia.to_parquet("data_lake/gold/fato_energia.parquet", index= False)
        logger.info("Fato Carga Energia salva no Data Lake Gold")
        
    except Exception as e: 
        logger.error(f"Erro ao salvar Fato Carga Energia no Data Lake Gold: {e}")
    
    logger.info("Tabela fato criada / Colunas: %s, Linhas: %s", fato_energia.shape[1], len(fato_energia))

    return {
        "dim_tempo" : dim_tempo,
        "dim_subsistema" : dim_subsistema,
        "fato_carga_energia" : fato_energia
    }
    