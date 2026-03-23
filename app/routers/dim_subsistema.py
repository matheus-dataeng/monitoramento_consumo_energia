import logging as log 
from app.db.database import SessionLocal
from sqlalchemy import text 
from fastapi import APIRouter
from fastapi import HTTPException

router = APIRouter()
logger = log.getLogger(__name__)

@router.get("/")

def get_subsistemas():
    db = SessionLocal()
    
    try:
        query = text("SELECT * FROM dim_subsistema LIMIT 100")
        result = db.execute(query)
        subsistemas = result.mappings().all()
        
        logger.info("Consulta realizada na tabela dim_subsistema")
        return subsistemas
    
    except Exception as e:
        logger.error(f"Erro ao consultar dim_subsistema: {e}")
        return {"erro" : "Falha ao consultar dados na tabela dim_subsistema"}

    finally:
        db.close()

        
@router.get("/{subsistema}")

def get_subsistemas_sigla(subsistema: str):
    db = SessionLocal()
    
    try:
        query = text(
            '''
            SELECT 
                sub."Subsistema",
                fat."Carga_energia"
            FROM fato_carga_energia AS fat
            LEFT JOIN dim_subsistema AS sub
                ON fat."Id_subsistema" = sub."Id_subsistema"
            WHERE sub."Subsistema" = :subsistema
            
            '''    
        )
        
        result = db.execute(query, {"subsistema" : subsistema})
        subsistema_sigla = result.mappings().first()
        
        if not subsistema_sigla:
            logger.error(f"Registros de {subsistema} não encontrados")
            raise HTTPException(status_code=404, detail=f"Subsistema {subsistema} não encontrado")
        
        logger.info("Consulta realizada na tabela dim_subsistema")
        return subsistema_sigla
    
    except Exception as e:
        logger.error(f"Erro ao consultar {subsistema} na tabela dim_subsistema: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar {subsistema}")
        
    
    finally:
        db.close()
            
            
        
    
    
    


    
    

        
    