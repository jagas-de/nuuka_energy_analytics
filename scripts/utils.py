import requests
import time,datetime
import random,string


from postgress_db_connect import get_db_connection


LIST_URL="https://helsinki-openapi.nuuka.cloud/api/v1.0/Property/List"
ENERGY_DATA_URL = "https://helsinki-openapi.nuuka.cloud/api/v1.0/EnergyData/Daily/ListByProperty"

def insert_audit_log(status):
    try:
        print("insert_audit_log")
        conn = get_db_connection()
        cursor = conn.cursor()
        timestamp = int(time.time() * 1000)  # Milliseconds
        random_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5)) 
        insert_query = """
            INSERT INTO nuuka_energy_test.audit_log(run_id,run_date, msg)
            VALUES (%s, %s,%s)
        """

        cursor.execute(insert_query, (random_id,datetime.date.today(),status))

        conn.commit()
        cursor.close()
        conn.close()
        print("message: audit data stored successfully.")   
        
    
    except Exception as e:
        print({"error": str(e)})
        raise(e)
    
def get_last_successful_run_date():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
            SELECT run_date
            FROM nuuka_energy_test.audit_log
            WHERE msg = 'SUCCESS'
            ORDER BY run_date DESC
            LIMIT 1;
        """
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            print("         start incrmental loading                ")
            return result[0]  
        else:
            print("         start initial loading                  ")
            return None  

    except Exception as e:
        print(f"Error fetching last successful run date: {e}")
        return None

    finally:
        cursor.close()
        conn.close()


def get_properties():
    response = requests.get(LIST_URL)
    return response.json()


def fetch_energy_data(property_name: str,start_time:str,end_time:str):

    params = {
        "Record": "LocationName",
        "SearchString": property_name,
        "ReportingGroup": "Electricity",
        "StartTime": start_time.strftime('%Y-%m-%d'),
        "EndTime": end_time.strftime('%Y-%m-%d')
    }
    
    response = requests.get(ENERGY_DATA_URL, params=params)

    if response.status_code != 200:
        print(f"error Failed to fetch energy data of lcoation {property_name}: {response.status_code}")
        return None
    print(f"processing energy data of lcoation {property_name}")
    return response.json()