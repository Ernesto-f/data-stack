from fastapi import (
    Depends,
    FastAPI,
)

from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import (
    db_table,
    get_session,
)

from uuid import UUID

from fastapi import HTTPException, status

from sqlalchemy.exc import DBAPIError

from app.schemas import (
    ProjectCreate,
    ProjectUpdate,
    ActivityCreate,
    ActivityUpdate,
    ProjectMemberCreate,
    ProjectCommentCreate,
    ProjectSuccessMetricCreate,
    ProjectSuccessMetricUpdate,
)

# ============================================================
# CRIA A APLICAÇÃO FASTAPI
# ============================================================

app = FastAPI(
    title="GSunnex API",
    version="0.1.0",
)


# ============================================================
# CORS
# ============================================================

# Como seu frontend é Flutter Web, o navegador pode considerar
# Flutter e API como origens diferentes.
#
# Por enquanto vamos permitir qualquer origem.
#
# NÃO É a configuração que usaríamos posteriormente
# em produção.
#
app.add_middleware(
    CORSMiddleware,

    allow_origins=["*"],

    # Não estamos usando cookies/JWT neste momento.
    allow_credentials=False,

    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# FUNÇÃO AUXILIAR
# ============================================================

async def select_all(
    session: AsyncSession,
    table_name: str,
):
    """
    Executa:

        SELECT * FROM tabela

    e transforma cada linha do PostgreSQL
    em um dicionário Python.

    Depois o FastAPI transformará o resultado em JSON.
    """

    query = text(
        f"""
        SELECT *
        FROM {db_table(table_name)}
        """
    )

    result = await session.execute(query)

    return [
        dict(row)
        for row in result.mappings().all()
    ]


# ============================================================
# HEALTHCHECK SIMPLES
# ============================================================

@app.get("/health")
async def health():
    """
    Este endpoint NÃO consulta o banco.

    Ele apenas confirma:

        "O FastAPI está executando?"
    """

    return {
        "status": "ok"
    }


# ============================================================
# HEALTHCHECK DO BANCO
# ============================================================

@app.get("/health/db")
async def health_database(
    session: AsyncSession = Depends(get_session),
):
    """
    Esse endpoint realmente consulta o PostgreSQL.

    Portanto:

    /health
        testa FastAPI

    /health/db
        testa FastAPI + PostgreSQL
    """

    result = await session.scalar(
        text("SELECT 1")
    )

    return {
        "status": "ok",
        "database": result == 1,
    }


# ============================================================
# USUÁRIOS
# ============================================================

@app.get("/api/v1/users")
async def list_users(
    session: AsyncSession = Depends(get_session),
):

    users = await select_all(
        session,
        "users",
    )

    return jsonable_encoder(users)


# ============================================================
# PROJETOS
# ============================================================

@app.get("/api/v1/projects")
async def list_projects(
    session: AsyncSession = Depends(get_session),
):

    projects = await select_all(
        session,
        "projects",
    )

    return jsonable_encoder(projects)

# ============================================================
# BUSCAR UM PROJETO
# ============================================================

@app.get("/api/v1/projects/{project_id}")
async def get_project(
    project_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    return await select_by_id(
        session,
        "projects",
        project_id,
    )


# ============================================================
# CRIAR PROJETO
# ============================================================

@app.post(
    "/api/v1/projects",
    status_code=status.HTTP_201_CREATED,
)
async def create_project(
    project: ProjectCreate,
    session: AsyncSession = Depends(get_session),
):
    return await insert_record(
        session,
        "projects",
        project.model_dump(),
    )


# ============================================================
# EDITAR PROJETO
# ============================================================

@app.patch("/api/v1/projects/{project_id}")
async def update_project(
    project_id: UUID,
    project: ProjectUpdate,
    session: AsyncSession = Depends(get_session),
):
    changes = project.model_dump(
        exclude_unset=True
    )

    return await update_record(
        session,
        "projects",
        project_id,
        changes,
    )


# ============================================================
# EXCLUIR PROJETO
# ============================================================

@app.delete("/api/v1/projects/{project_id}")
async def delete_project(
    project_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    return await delete_record(
        session,
        "projects",
        project_id,
    )

# ============================================================
# ATIVIDADES
# ============================================================

@app.get("/api/v1/activities")
async def list_activities(
    session: AsyncSession = Depends(get_session),
):

    activities = await select_all(
        session,
        "activities",
    )

    return jsonable_encoder(activities)

@app.get("/api/v1/activities/{activity_id}")
async def get_activity(
    activity_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    return await select_by_id(
        session,
        "activities",
        activity_id,
    )


@app.post(
    "/api/v1/activities",
    status_code=status.HTTP_201_CREATED,
)
async def create_activity(
    activity: ActivityCreate,
    session: AsyncSession = Depends(get_session),
):
    return await insert_record(
        session,
        "activities",
        activity.model_dump(),
    )


@app.patch("/api/v1/activities/{activity_id}")
async def update_activity(
    activity_id: UUID,
    activity: ActivityUpdate,
    session: AsyncSession = Depends(get_session),
):
    return await update_record(
        session,
        "activities",
        activity_id,
        activity.model_dump(
            exclude_unset=True
        ),
    )


@app.delete("/api/v1/activities/{activity_id}")
async def delete_activity(
    activity_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    return await delete_record(
        session,
        "activities",
        activity_id,
    )


# ============================================================
# BOOTSTRAP DO FLUTTER
# ============================================================

@app.get("/api/v1/bootstrap")
async def bootstrap(
    session: AsyncSession = Depends(get_session),
):
    """
    Esse será o endpoint mais importante inicialmente.

    Seu Flutter hoje faz:

        MockData.users()
        MockData.projects()
        MockData.activities()
        ...

    Vamos substituir isso por UMA chamada HTTP:

        GET /api/v1/bootstrap

    A resposta conterá todo o conjunto inicial
    necessário para montar a aplicação.
    """

    users = await select_all(
        session,
        "users",
    )

    projects = await select_all(
        session,
        "projects",
    )

    activities = await select_all(
        session,
        "activities",
    )

    members = await select_all(
        session,
        "project_members",
    )

    comments = await select_all(
        session,
        "project_comments",
    )

    metrics = await select_all(
        session,
        "project_success_metrics",
    )


    response = {
        "users": users,
        "projects": projects,
        "activities": activities,
        "project_members": members,
        "project_comments": comments,
        "project_success_metrics": metrics,
    }


    # UUID, datetime, Decimal etc. são convertidos
    # para tipos compatíveis com JSON.
    return jsonable_encoder(response)

# ============================================================
# BUSCAR UM REGISTRO PELO ID
# ============================================================

async def select_by_id(
    session: AsyncSession,
    table_name: str,
    row_id: UUID,
):
    query = text(
        f"""
        SELECT *
        FROM {db_table(table_name)}
        WHERE id = :id
        """
    )

    result = await session.execute(
        query,
        {
            "id": row_id,
        },
    )

    row = result.mappings().first()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Registro não encontrado.",
        )

    return jsonable_encoder(
        dict(row)
    )


# ============================================================
# INSERT
# ============================================================

async def insert_record(
    session: AsyncSession,
    table_name: str,
    data: dict,
):
    """
    Recebe:

        {
            "name": "...",
            "status": "...",
            ...
        }

    E automaticamente monta:

        INSERT INTO tabela (
            name,
            status,
            ...
        )
        VALUES (
            :name,
            :status,
            ...
        )
        RETURNING *

    RETURNING * faz o PostgreSQL devolver o registro
    que acabou de criar, incluindo o UUID gerado.
    """

    columns = ", ".join(
        f'"{column}"'
        for column in data.keys()
    )

    parameters = ", ".join(
        f":{column}"
        for column in data.keys()
    )

    query = text(
        f"""
        INSERT INTO {db_table(table_name)}
        (
            {columns}
        )
        VALUES
        (
            {parameters}
        )
        RETURNING *
        """
    )

    try:
        result = await session.execute(
            query,
            data,
        )

        row = result.mappings().one()

        await session.commit()

        return jsonable_encoder(
            dict(row)
        )

    except DBAPIError as error:
        await session.rollback()

        raise HTTPException(
            status_code=400,

            # Durante desenvolvimento é útil enxergar
            # o erro retornado pelo PostgreSQL.
            detail=str(error.orig),
        ) from error


# ============================================================
# UPDATE
# ============================================================

async def update_record(
    session: AsyncSession,
    table_name: str,
    row_id: UUID,
    changes: dict,
):
    """
    Atualiza somente os campos recebidos.

    Exemplo:

        {
            "name": "Novo nome",
            "progress": 50
        }

    gera:

        SET
            name = :name,
            progress = :progress
    """

    # Se nenhum campo foi enviado,
    # simplesmente devolvemos o registro atual.
    if not changes:
        return await select_by_id(
            session,
            table_name,
            row_id,
        )

    set_clause = ", ".join(
        f'"{column}" = :{column}'
        for column in changes.keys()
    )

    parameters = {
        **changes,
        "id": row_id,
    }

    query = text(
        f"""
        UPDATE {db_table(table_name)}
        SET
            {set_clause}
        WHERE id = :id
        RETURNING *
        """
    )

    try:
        result = await session.execute(
            query,
            parameters,
        )

        row = result.mappings().first()

        if row is None:
            await session.rollback()

            raise HTTPException(
                status_code=404,
                detail="Registro não encontrado.",
            )

        await session.commit()

        return jsonable_encoder(
            dict(row)
        )

    except HTTPException:
        raise

    except DBAPIError as error:
        await session.rollback()

        raise HTTPException(
            status_code=400,
            detail=str(error.orig),
        ) from error


# ============================================================
# DELETE
# ============================================================

async def delete_record(
    session: AsyncSession,
    table_name: str,
    row_id: UUID,
):
    query = text(
        f"""
        DELETE FROM {db_table(table_name)}
        WHERE id = :id
        RETURNING id
        """
    )

    try:
        result = await session.execute(
            query,
            {
                "id": row_id,
            },
        )

        deleted_id = result.scalar_one_or_none()

        if deleted_id is None:
            await session.rollback()

            raise HTTPException(
                status_code=404,
                detail="Registro não encontrado.",
            )

        await session.commit()

        return {
            "deleted": True,
            "id": str(deleted_id),
        }

    except HTTPException:
        raise

    except DBAPIError as error:
        await session.rollback()

        raise HTTPException(
            status_code=400,
            detail=str(error.orig),
        ) from error

# =================================================
#                 project-members
# =================================================
@app.post(
    "/api/v1/project-members",
    status_code=status.HTTP_201_CREATED,
)
async def create_project_member(
    member: ProjectMemberCreate,
    session: AsyncSession = Depends(get_session),
):
    return await insert_record(
        session,
        "project_members",
        member.model_dump(),
    )


@app.delete(
    "/api/v1/project-members/{member_id}"
)
async def delete_project_member(
    member_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    return await delete_record(
        session,
        "project_members",
        member_id,
    )

# =================================================
#                 project-comments
# =================================================
@app.post(
    "/api/v1/project-comments",
    status_code=status.HTTP_201_CREATED,
)
async def create_project_comment(
    comment: ProjectCommentCreate,
    session: AsyncSession = Depends(get_session),
):
    return await insert_record(
        session,
        "project_comments",
        comment.model_dump(),
    )


@app.delete(
    "/api/v1/project-comments/{comment_id}"
)
async def delete_project_comment(
    comment_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    return await delete_record(
        session,
        "project_comments",
        comment_id,
    )

# =================================================
#                 project-success-metrics
# =================================================
@app.post(
    "/api/v1/project-success-metrics",
    status_code=status.HTTP_201_CREATED,
)
async def create_project_success_metric(
    metric: ProjectSuccessMetricCreate,
    session: AsyncSession = Depends(get_session),
):
    return await insert_record(
        session,
        "project_success_metrics",
        metric.model_dump(),
    )


@app.patch(
    "/api/v1/project-success-metrics/{metric_id}"
)
async def update_project_success_metric(
    metric_id: UUID,
    metric: ProjectSuccessMetricUpdate,
    session: AsyncSession = Depends(get_session),
):
    return await update_record(
        session,
        "project_success_metrics",
        metric_id,
        metric.model_dump(
            exclude_unset=True
        ),
    )


@app.delete(
    "/api/v1/project-success-metrics/{metric_id}"
)
async def delete_project_success_metric(
    metric_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    return await delete_record(
        session,
        "project_success_metrics",
        metric_id,
    )

