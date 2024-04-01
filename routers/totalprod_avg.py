from fastapi import APIRouter
from sqlalchemy import text
from db import DW

router = APIRouter(
    prefix='/api/measurement/production/total/avg',
    tags=['Production - Total - Avg']
)

