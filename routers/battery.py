from fastapi import APIRouter
from sqlalchemy import text
from db import DW


router = APIRouter(
    prefix='/api/measurement/battery',
    tags=['Battery']
)


# Palauttaa uusimmat tilastot akun tiedoista
@router.get("/current")
async def get_most_recent_values_from_battery(dw: DW):
    _query = text(
        "SELECT s.sensor_name AS sensor, m.value AS value "
        "FROM measurements_fact AS m JOIN dates_dim AS d ON d.date_key = m.date_key "
        "JOIN sensors_dim AS s ON s.sensor_key = m.sensor_key "
        "WHERE s.device_id = 'TB_batterypack' AND m.date_key = ("
        "   SELECT MAX(date_key) "
        "   FROM measurements_fact "
        "   WHERE sensor_key = s.sensor_key "
        ") "
        "ORDER BY s.sensor_id;")
    rows = dw.execute(_query)
    data = rows.mappings().all()

    return {'current_battery_stats': data}
