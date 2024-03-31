from fastapi import FastAPI
from routers import battery, consumption, temperature, production


app = FastAPI()
app.include_router(battery.router)
app.include_router(consumption.router)
app.include_router(temperature.router)
app.include_router(production.router)
