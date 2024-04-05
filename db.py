import os
from typing import Annotated

from dotenv import load_dotenv
from fastapi import Depends
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

# Aukaistaan .env filu ja haetaan DW (olap) ja DB (oltp)
load_dotenv(dotenv_path=".env")

# DW käyttää OLAP tietokantaa
dw = os.environ.get("DW")
dw_engine = create_engine(dw)
dw_session = sessionmaker(bind=dw_engine)

# DB käyttää OLTP tietokantaa
db = os.environ.get("DB")
db_engine = create_engine(db)
db_session = sessionmaker(bind=db_engine)


def get_dw():
    _dw = None
    try:
        _dw = dw_session()
        yield _dw
    finally:
        if _dw is not None:
            _dw.close()


def get_db():
    _db = None
    try:
        _db = db_session()
        yield _db
    finally:
        if _db is not None:
            _db.close()


# Nämä importataan sinne, missä databaseja tarvii
DW = Annotated[Session, Depends(get_dw)]
DB = Annotated[Session, Depends(get_db)]
