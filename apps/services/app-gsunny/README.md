ARQUITETURA QUE SEMPRE SE REPETE, SÓ COPIA E COLA.

app/
├── main.py
├── api/
│   └── router.py
├── core/
│   └── config.py
├── db/
│   ├── base.py
│   └── session.py
└── modules/


APOS COPIAR, MUDE: 
    - TITULO DENTRO DE MAIN
    - CRIE O USUARIO PRA NOVA API NO BANCO
    - COLOQUE A CREDENCIAL E A VARIAVEL EM service/infra/.env
        ex: DATABASE_URL_GSUNNY=postgresql+psycopg:
            fica -> DATABASE_URL_NOMEDONOVOAPP=postgresql+psycopg:
    - NOME DA STRING EM app/db/session.py
        ex: sai DATABASE_URL_GSUNNY, entra DATABASE_URL_NOMEDONOVOAPP


OQUE IMPORTA NA API!!! 

GET /api/v1/pedidos/listarpedidos
                ↓
main.py
                ↓
app/api/router.py
                ↓
modules/pedidos/router.py
                ↓
FastAPI valida os parâmetros
                ↓
Depends(get_session) cria a sessão
                ↓
função listar_pedidos() do router
                ↓
service.listar_pedidos()
                ↓
repository.listar_pedidos()
                ↓
Model Pedido + AsyncSession
                ↓
SQLAlchemy → Psycopg → PostgreSQL
                ↓
resultado volta para o repository
                ↓
resultado volta para o service
                ↓
resultado volta para o router
                ↓
PedidoListResponse valida e serializa
                ↓
JSON é enviado ao cliente


