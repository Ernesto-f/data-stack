from fastapi import FastAPI, HTTPException, Depends, Header, APIRouter
from contextlib import contextmanager
import psycopg
from psycopg.rows import namedtuple_row
from pathlib import Path
from app.models import ContainersResponse, CommentsOptionsResponse, CommentsCreate, CommentsDelete, CommentsUpdate
from fastapi.middleware.cors import CORSMiddleware

API_PREFIX = "/fmdfiltercount/api"

app = FastAPI(
    title="Filter Counting API",
    description="API para registro de comentários em contêineres — Grafana Plugin",
    version="1.0.0",
    root_path="/fmdfiltercount/api"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
    
PG_CONN_STR = "postgresql://usr_apex_nodered:Fantasy_nodered@10.42.34.205:5432/apex_dw_raw"

@contextmanager
def conn():
    c = psycopg.connect(
        PG_CONN_STR,  
        row_factory=namedtuple_row,
    )

    try:
        yield c
    except psycopg.OperationalError as e:
        print(f"O erro é {e}")
        c.rollback()
        raise
    finally:
        c.close()

      
@app.get("/containers", response_model=list[ContainersResponse])
def get_conteiners():

    ## Ler arquivo SQL
    sql_file = Path(__file__).parent / "query_containers.sql"
    with open(sql_file, 'r', encoding='utf-8') as arquivo:
        conteudo = arquivo.read()
    ##

    with conn() as cn:
        sql = conteudo
        
        try:
            
            cur = cn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
        except psycopg.OperationalError as e:
               print(f"O erro é {e}")
    if not rows:
        return None

    colunas = ["Rank","achid","transactioncode","srcproductno","paradoem","modulo","workday","srcwarehouselocation","dstwarehouselocation","container","incontainer","containerclassid","oldquantity","newquantity","quantityadjusted","uomcode","createdon","transactioncontext","wiporderno","wipordertype","nam_comments","cod_id","dezhoras","agora","atualizacao"]
    dicionario = [dict(zip(colunas, item)) for item in rows]
    return dicionario


@app.get("/comments", response_model=list[CommentsOptionsResponse])
def get_commentsoptions():

    with conn() as cn:
        sql = """
            SELECT
                nam_comments_type,
                nam_comments
            FROM
                udi_development.tb_comments_options
            """       
        try:            
            cur = cn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
        except psycopg.OperationalError as e:
               print(f"O erro é {e}")
    if not rows:
        return None

    colunas = ["nam_comments_type","nam_comments"]
    dicionario = [dict(zip(colunas, item)) for item in rows]
    return dicionario

@app.post("/comments", status_code=201)
def createcomment(payload: CommentsCreate):

    with conn() as cn:

        sqlcheck = f"""
                    SELECT COUNT(cod_id) AS resultado FROM udi_development.tb_comments 
                        WHERE nam_comments_type = 'DSH' AND val_container_number = %s
        """
         
        sql = f"""
                INSERT INTO udi_development.tb_comments 
                    (val_container_number, nam_comments_type, nam_comments, usr_grafana) 
                    VALUES 
                    (%s, %s, %s, %s)
                    RETURNING cod_id
            """     
        try:            
            cur = cn.cursor()

            # Consulta os comentários para verificar se já existe cadastrado
            cur.execute(
                sqlcheck,[
                 payload.val_container_number,]
            )            
            row_element = cur.fetchone()

            if  row_element[0] > 0:
                raise HTTPException(status_code='',  detail="Já cadastrado para este contêiner")

            # Executa a inclusão
            cur.execute(
                sql,[
                 payload.val_container_number,
                payload.nam_comments_type,
                payload.nam_comments,
                payload.usr_grafana,]
            )
            row = cur.fetchone()
            cn.commit()            

        except Exception as e:
            cn.rollback()
            raise HTTPException(status_code=500, detail=f"Erro ao gravar comentário: {str(e)}")

    return {"cod_record": row[0], "message": ""}

@app.delete("/comments", status_code=200)
def deletecomment(payload: CommentsDelete):

    with conn() as cn:
        sql = f"""
                DELETE FROM udi_development.tb_comments
                    WHERE cod_id=%s;
            """     
        try: 
            cur = cn.cursor()
            cur.execute(
                sql,[
                 payload.cod_id,
                 ]
            )
            cn.commit()
            

        except Exception as e:
            cn.rollback()
            raise HTTPException(status_code=500, detail=f"Erro ao excluir comentário: {str(e)}")

    return True

@app.patch("/comments", status_code=202)
def updatecomment(payload: CommentsUpdate):

    with conn() as cn:

        sql = f"""
                UPDATE udi_development.tb_comments
                    SET nam_comments=%s, usr_grafana=%s
                    WHERE cod_id=%s;
            """     
        try:            
            cur = cn.cursor()

            # Executa a alteração
            cur.execute(
                sql,[
                payload.nam_comments,
                payload.usr_grafana,
                payload.cod_id,]
            )
            cn.commit()            

        except Exception as e:
            cn.rollback()
            raise HTTPException(status_code=500, detail=f"Erro ao gravar comentário: {str(e)}")

    return True

@app.get("/health", tags=["System"])
async def health():
    return {"status": "Servidor on-line 🟢"}