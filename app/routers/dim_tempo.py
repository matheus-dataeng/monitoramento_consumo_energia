from fastapi import APIRouter
from sqlalchemy import text 
from app.db.database import SessionLocal
import logging as log 
from fastapi import HTTPException

router = APIRouter()
logger = log.getLogger(__name__)

@router.get("/")

def get_tempo():
    db = SessionLocal()
    
    try:
        result = db.execute(text("SELECT * FROM dim_tempo LIMIT 100"))
        logger.info("Consulta realizada na tabela dim_tempo")
        return result.mappings().all()
    
    except Exception as e:
        logger.error(f"Erro ao consultar dim_tempo: {e}")
        raise HTTPException(status_code=500, detail="Erro ao cosultar dados da tabela")
       
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
        
        if not tempo:
            logger.warning(f"Registros do mês {mes} não encontrados")
            raise HTTPException(status_code= 404, detail= f"Registros do mês {mes} não encontrados")
        
        logger.info(f"Consulta realizada ao dados do mês: {mes}")
        return tempo
    
    except HTTPException:
        raise  
    
    except Exception as e:
        logger.error(f"Erro ao consultar mes: {mes}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar dados do mês: {mes}")
    
    finally: 
        db.close()
    
    