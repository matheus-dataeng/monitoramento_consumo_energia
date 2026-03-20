from fastapi import APIRouter
from sqlalchemy import text 
from app.db.database import SessionLocal
import logging as log 

router = APIRouter()
logger = log.getLogger(__name__)

@router.get("/")

def get_tempo():
    db = SessionLocal()
    
    try:
        query = text("SELECT * FROM dim_tempo LIMIT 100")
        result = db.execute(query)
        tempo = result.mappings().all()
        
        logger.info("Consulta realizada na tabela dim_tempo")
        return tempo 
    
    except Exception as e:
        logger.error(f"Erro ao consultar dim_tempo: {e}")
        return {"erro" : "Falha ao consultar dados na tabela dim_tempo"}
        

    finally:
        db.close()

@router.get("/{mes}")

def get_tempo_mes(mes: int):
    db = SessionLocal()
    
    try:
        query = text(
            '''
            SELECT 
                temp."Mes",
                fat."Carga_energia"
            FROM fato_carga_energia AS fat
            LEFT JOIN dim_tempo AS temp
	            ON fat."Id_tempo" = temp."Id_tempo"
             WHERE temp."Mes" = :mes
             
            '''     
        )
        
        result = db.execute(query, {"mes" : mes})
        tempo = result.mappings().all()
        
        logger.info("Consulta realizada na tabela dim_tempo para Ano e Mês")
        return tempo
    
    except Exception as e:
        logger.info(f"Erro ao consultar mes: {mes}: {e}")
        return {"erro" : "Falha ao consultar dados Ano e Mês"}
    
    finally: 
        db.close()
    
    