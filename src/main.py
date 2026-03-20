import logging as log 
from utils.logger_setup import setup_log
from bronze.extract import extract
from silver.transform import transform
from gold.build_metrics import build_metrics
from gold.load import load

setup_log()
logger = log.getLogger(__name__)

def main():
    logger.info("Iniciando Pipeline")
    
    df_raw = extract()
    df_silver = transform(df_raw)
    df_gold = build_metrics(df_silver)
    load(
        df_gold["dim_tempo"],
        df_gold["dim_subsistema"],
        df_gold["fato_carga_energia"]
    )
    
    logger.info("Pipeline finalizado")
    
if __name__ == "__main__":
    main()