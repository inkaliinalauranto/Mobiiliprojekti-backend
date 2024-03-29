from fastapi import FastAPI
from sqlalchemy import text
from datetime import datetime

from db import DW

app = FastAPI()


"""  
how to fastapi

@app.get("/endpoint/merkkijono/{jokinarvo}")
async def kuvaava_nimi_tähän_tyyliin_koska_menee_docs(dw: DW tai db: DB, jokinarvo: int):
    
    _query_str = ("SELECT value FROM measurements_fact AS m")
    
    _query = text(_query_str)
    rows = dw. tai db.execute(_query, {"jokinarvo": jokinarvo})
    
    # mappings tekee jokaisesta tuloksen rivistä dictionaryn, 
    # jossa keynä on sql columnin nimi ja tekee niistä listan esim:
    # [
    #   {
    #     "consumption" : 40
    #   },
    #   {
    #     "consumption" : 50
    #   }
    # ]
    
    data = rows.mappings().all()

    return {'dictionary_key': data}
"""



# Akun kunto palauttaa null tällä hetkellä,
# ehkä pitää poistaa niin saadaan koodista siistimpi.
# Ja samalla varmaan poistaa temp, koska sinänsä aika turha tieto.

# Palauttaa akun uusimman tiedon varauksesta, kunnosta, lämpötilasta
@app.get("/api/measurement/battery")
async def get_battery_statistics(dw: DW):
    _query_soc = ("SELECT value as 'SoC' FROM measurements_fact AS m "
                  "JOIN dates_dim AS d ON d.date_key = m.date_key "
                  "JOIN sensors_dim AS s ON s.sensor_key = m.sensor_key "
                  "WHERE s.device_id = 'TB_batterypack' AND s.sensor_id = 'soc' "
                  "ORDER BY m.date_key DESC LIMIT 1;")

    _query_soh = ("SELECT value as 'SoH' FROM measurements_fact AS m "
                  "JOIN dates_dim AS d ON d.date_key = m.date_key "
                  "JOIN sensors_dim AS s ON s.sensor_key = m.sensor_key "
                  "WHERE s.device_id = 'TB_batterypack' AND s.sensor_id = 'soh' "
                  "ORDER BY m.date_key DESC LIMIT 1;")

    _query_temp = ("SELECT value as 'Temperature' FROM measurements_fact AS m "
                  "JOIN dates_dim AS d ON d.date_key = m.date_key "
                  "JOIN sensors_dim AS s ON s.sensor_key = m.sensor_key "
                  "WHERE s.device_id = 'TB_batterypack' AND s.sensor_id = 'temperature' "
                  "ORDER BY m.date_key DESC LIMIT 1;")

    _query = text(_query_soc)
    rows = dw.execute(_query)
    soc = rows.mappings().all()

    _query = text(_query_soh)
    rows = dw.execute(_query)
    soh = rows.mappings().all()

    _query = text(_query_temp)
    rows = dw.execute(_query)
    temp = rows.mappings().all()

    for i in soc:
        print(i)
        soc = i.get("SoC")

    for i in soh:
        print(i)
        soh = i.get("SoH")

    for i in temp:
        print(i)
        temp = i.get("Temperature")

    data = {"soc": soc, "soh": soh, "temp": temp}

    return {'battery_statistics': data}


# Haetaan päiväkohtainen kokonaistuotto tunneittain ryhmiteltynä:
@app.get("/api/measurement/total_production/day/{date}")
async def get_total_production_statistics_hourly_for_a_day(dw: DW, date: str):
    _query = text("SELECT SUM(p.value) AS total_production, d.hour FROM `productions_fact` p LEFT JOIN dates_dim d ON p.date_key = d.date_key WHERE CONCAT_WS('-', d.year, d.month, d.day) = :date GROUP BY d.hour;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}


# Haetaan viikkokohtainen kokonaistuotto päivittäin ryhmiteltynä:
@app.get("/api/measurement/total_production/week/{date}")
async def get_total_production_statistics_daily_for_a_week(dw: DW, date: str):
    _query = text("SELECT DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) as day, SUM(p.value) AS total_production FROM `productions_fact` p LEFT JOIN dates_dim d ON p.date_key = d.date_key WHERE DATE(TIMESTAMP(CONCAT_WS('-', d.year, d.month, d.day))) BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND :date GROUP BY d.day;")
    rows = dw.execute(_query, {"date": date})
    data = rows.mappings().all()
    return {"data": data}
