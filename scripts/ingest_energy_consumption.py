#from fastapi import FastAPI
import time,datetime
from datetime import datetime
from postgress_db_connect import get_db_connection
from utils import fetch_energy_data





def fetch_and_insert_energy_data(data,start_time,end_time):
        # Store into PostgreSQL
    try:
        input_data = fetch_energy_data(data,start_time,end_time)
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO nuuka_energy_test.energy_usage (reportinggroup, create_dttm, value, unit, location,insert_dttm)
            VALUES (%s, %s, %s, %s, %s,%s)
        """

        for record in input_data:
            cursor.execute(insert_query, (
                record.get("reportingGroup"),
                record.get("timestamp"),
                record.get("value"),
                record.get("unit"),
                record.get("locationName"),
                datetime.today(),

            ))

        conn.commit()
        cursor.close()
        conn.close()
        time.sleep(2)
        return {"message": "Energy data stored successfully."}

    except Exception as e:
        return {"error": str(e)}
    
    
 

 