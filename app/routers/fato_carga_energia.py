import logging as log
from sqlalchemy import text 
from app.db.database import SessionLocal
from fastapi import APIRouter

router = APIRouter()
logger = log.getLogger(__name__)

@router.get("/")

def get_fato():
    db = SessionLocal()
    
    try:
        query = text("SELECT * FROM fato_carga_energia LIMIT 100")
        result = db.execute(query)
        fato = result.mappings().all()
        
        logger.info("Consulta realizada na tabela fato_carga_energia")
        return fato
    
    except Exception as e:
        logger.error(f"Erro ao consultar tabela fato {e}")    
        return {"erro" : "Falha ao consultar dados na tabela fato"}
    
    finally:
        db.close()

@router.get("/ids/{id_tempo}/{id_subsistema}")

def get_fato_ids(id_tempo: int, id_subsistema: int):
    db = SessionLocal()
    
    try:
        query = text(
            '''
            SELECT 
                temp."Ano",
                temp."Mes",
                sub."Subsistema",
                sub."Sigla",
                fat."Carga_energia"
            FROM fato_carga_energia AS fat
            JOIN dim_tempo AS temp
                ON fat."Id_tempo" = temp."Id_tempo"
            JOIN dim_subsistema AS sub
                ON fat."Id_subsistema" = sub."Id_subsistema"
            WHERE fat."Id_tempo" = :id_tempo
            AND fat."Id_subsistema" = :id_subsistema  
            '''
            
        )
        
        result = db.execute(query, {"id_tempo" : id_tempo, "id_subsistema" : id_subsistema})
        fato_ids = result.mappings().all()
        
        if not fato_ids:
            logger.warning(f"Registros {id_tempo} e {id_subsistema} não encontrados")
            return {"erro" : "Registros não encontrados"}
        
        logger.info(f"Consulta realizada aos registros {id_tempo} e {id_subsistema}")
        return fato_ids
    
    except Exception as e:
        logger.error(f"Erro ao consultar {id_tempo} e {id_subsistema}: {e}")
        return {"erro" : "Falha ao encontrar os registros"}
    
    finally:
        db.close()

@router.get("/datas/{data_start}/{data_end}")

def get_fato_ano(data_start: str, data_end: str):
    db = SessionLocal()
    
    try:
        query = text(
            '''
        SELECT 
            temp."Data",
            temp."Dia_semana",
            temp."Trimestre",
            sub."Subsistema",
            fat."Carga_energia"
        FROM fato_carga_energia AS fat
        JOIN dim_tempo AS temp
            ON fat."Id_tempo" = temp."Id_tempo"
        JOIN dim_subsistema AS sub
            ON fat."Id_subsistema" = sub."Id_subsistema"
        WHERE temp."Data" BETWEEN :data_start AND :data_end 
	           
            ''' 
        )
        
        result = db.execute(query, {"data_start" : data_start, "data_end" : data_end})    
        fato_data = result.mappings().all()
        
        if not fato_data:
            logger.warning(f"Registro de {data_start} e {data_end} não encontrados")
            return {"erro" : "Registros não encontrados"}    
        
        logger.info(f"Consulta realizada nos registros {data_start} e {data_end}")
        return fato_data
        
    except Exception as e:
        logger.error(f"Erro ao consultar {data_start} e {data_end}: {e}")
        return {"erro" : "Registros não encontrados"}
    
    finally: 
        db.close()