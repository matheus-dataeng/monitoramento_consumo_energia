import logging as log
from app.db.database import SessionLocal
from sqlalchemy import text
from fastapi import APIRouter
from fastapi import HTTPException

router = APIRouter()
logger = log.getLogger(__name__)

@router.get("/carga-por-subsistema/{subsistema}")

def metrics_subsistema(subsistema: str):
    db = SessionLocal()
    
    try:
        query = text(
            '''
            SELECT 
                sub."Subsistema",
                sub."Sigla",
                ROUND(SUM(fat."Carga_energia")::numeric, 2) AS carga_energia_total
            FROM fato_carga_energia AS fat
            JOIN dim_subsistema AS sub
                ON fat."Id_subsistema" = sub."Id_subsistema"
            WHERE sub."Subsistema" = :subsistema
            GROUP BY sub."Subsistema", sub."Sigla"
            
            '''
        )

        result = db.execute(query, {"subsistema" : subsistema})
        
        logger.info(f"Consulta realizada a carga por subsistema {subsistema}")
        return result.mappings().first()
    
    except Exception as e:
        logger.error(f"Erro ao consultar a carga do subsistema {subsistema}: {e}")
        raise HTTPException(status_code= 500, detail= f"Erro ao encontrar a carga por subsistema {subsistema}")
    
    finally:
        db.close()


@router.get("/carga-media/{mes}")

def get_metrics_mes(mes: str):
    db = SessionLocal()
    
    try:     
        query = text(
            '''
            SELECT 
                temp."Mes",
                temp."Trimestre",
                ROUND(AVG(fat."Carga_energia")::numeric, 2) AS media_carga_energia
            FROM fato_carga_energia AS fat
            JOIN dim_tempo AS temp
                ON fat."Id_tempo" = temp."Id_tempo"
            WHERE temp."Mes" = :mes    
            GROUP BY temp."Mes", temp."Trimestre"
        
            '''
        )
        
        result = db.execute(query, {"mes" : mes})
        
        logger.info(f"Consulta realizada a média do mes de {mes}")
        return result.mappings().first()

    except Exception as e:
        logger.error(f"Erro ao consultar a média do {mes}: {e}")
        raise HTTPException(status_code=500, detail= f"Erro ao encontrar a média do mes {mes}")
    
    finally:
        db.close()
        



    