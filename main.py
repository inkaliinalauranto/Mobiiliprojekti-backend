from fastapi import FastAPI
from routers import (battery, totalconsumpt, totalconsumpt_avg, totalconsumpt_sum,
                     temperature, windproduction, solarproduction, totalprod, totalprod_sum, totalprod_avg, auth)


app = FastAPI()
app.include_router(auth.router)
app.include_router(battery.router)
app.include_router(totalconsumpt.router)
app.include_router(totalconsumpt_avg.router)
app.include_router(totalconsumpt_sum.router)
app.include_router(temperature.router)
app.include_router(totalprod.router)
app.include_router(totalprod_sum.router)
app.include_router(totalprod_avg.router)
app.include_router(windproduction.router)
app.include_router(solarproduction.router)
