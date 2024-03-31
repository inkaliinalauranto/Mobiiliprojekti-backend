from fastapi import FastAPI
from routers import battery, totalconsumpt, totalconsumpt_avg, totalconsumpt_sum, temperature, production


app = FastAPI()
app.include_router(battery.router)
app.include_router(totalconsumpt.router)
app.include_router(totalconsumpt_avg.router)
app.include_router(totalconsumpt_sum.router)
app.include_router(temperature.router)
app.include_router(production.router)
