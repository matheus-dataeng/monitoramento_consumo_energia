import pandas as pd
import logging as log 

logger = log.getLogger(__name__)

def column_names(df: pd.DataFrame) -> pd.DataFrame:
    colunas_renomeadas = {
        "id_subsistema" : "Sigla",
        "nom_subsistema" : "Subsistema",
        "din_instante" : "Data" ,
        "val_cargaenergiamwmed" : "Carga_energia" 
    }
    
    df.columns = df.columns.str.strip()
    df.rename(columns= colunas_renomeadas, inplace= True)
    logger.info("Colunas renomeadas / Colunas: %s, Linhas: %s", df.shape[1], len(df))
    return df 

def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    df["Data"] = pd.to_datetime(df["Data"], errors= "coerce")
    df["Carga_energia"] = pd.to_numeric(df["Carga_energia"], errors= "coerce")
    
    logger.info("Valores convertidos / Linhas processadas: %s", len(df))
    return df 
    
def validate_columns(df:pd.DataFrame) -> pd.DataFrame:
    col_validacao = ["Data", "Carga_energia"]
    
    for col_invalidas in col_validacao:
        colunas_invalidas = int(df[col_invalidas].isna().sum())
        
        if colunas_invalidas:
            logger.warning("Valores invalidos na coluna %s: %s", col_invalidas, colunas_invalidas)
    
    carga_zerada = int((df["Carga_energia"] == 0).sum())
    
    if carga_zerada:
        logger.warning("Carga_energia zeradas: %s", carga_zerada)
    
    linhas_antes = len(df)
    
    df = df[
        (df["Data"].notna()) &
        (df["Carga_energia"].notna()) &
        (df["Carga_energia"] > 0)
    ]

    linhas_depois = len(df)
    linhas_removidas = linhas_antes - linhas_depois
    
    if linhas_removidas:
        logger.warning("Linhas removidas após tratativas: %s", linhas_removidas)
    else:
        logger.info("Nenhuma linha removida")
    
    logger.info("Colunas validadas / Linhas restantes: %s", len(df))
    return df 

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    linhas_antes = len(df)
    
    df = df.drop_duplicates(subset=["Sigla", "Data"])
    
    linhas_depois = len(df)
    duplicados_removidos = linhas_antes - linhas_depois
    
    if duplicados_removidos:
        logger.warning("Dados duplicados removidos: %s", duplicados_removidos)
    else:
        logger.info("Nenhum duplicado encontrado")
    
    logger.info("Linhas restantes: %s", len(df))
    return df

def create_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df["Ano"] = df["Data"].dt.year
    df["Mes"] = df["Data"].dt.month
    df["Dia"] = df["Data"].dt.day
    df["Trimestre"] = df["Data"].dt.quarter
    
    dias_semana = {
        "Monday" : "Segunda-Feira",
        "Tuesday" : "Terça-Feira",
        "Wednesday" : "Quarta-Feira",
        "Thursday" : "Quinta-Feira",
        "Friday" : "Sexta-Feira",
        "Saturday" : "Sábado",
        "Sunday" : "Domingo" 
    }

    df["Dia_semana"] = df["Data"].dt.day_name().replace(dias_semana)
    
    logger.info("Features temporais criadas")
    return df 

def transform(df:pd.DataFrame) -> pd.DataFrame:
    logger.info("Iniciando Transformações")
    
    df = column_names(df)
    df = convert_types(df)
    df = validate_columns(df)
    df = create_time_features(df)
    df = remove_duplicates(df)
    
    logger.info("Transformações realizadas / Colunas: %s, Linhas: %s", df.shape[1], len(df))
    
    try:
        df.to_parquet("data_lake/silver/carga_energia_tratada.parquet", index= False)
        logger.info("Arquivo salvo no Data Lake Silver com sucesso")
        
    except Exception as e:
        logger.error(f"Erro ao salvar no Data Lake Silver: {e}")
        raise 
    
    return df 