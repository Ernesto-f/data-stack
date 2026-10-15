from pydantic import BaseModel

class ContainersResponse(BaseModel):
    cod_id:         int | None = None
    container:      str
    nam_comments:   str | None = None
    Rank:           int
    srcproductno:   str

class CommentsOptionsResponse(BaseModel):
    nam_comments_type:      str
    nam_comments:           str

class CommentsCreate(BaseModel):
  val_container_number: str
  nam_comments_type:    str
  nam_comments:         str
  usr_grafana:          str | None = None

class CommentsUpdate(BaseModel):
  cod_id:               int
  nam_comments:         str
  usr_grafana:          str | None = None

class CommentsDelete(BaseModel):
  cod_id: int
