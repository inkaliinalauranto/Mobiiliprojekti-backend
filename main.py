from fastapi import FastAPI
from router import consumption_router, production_router, battery_router

app = FastAPI()
app.include_router(battery_router)
app.include_router(consumption_router)
app.include_router(production_router)
