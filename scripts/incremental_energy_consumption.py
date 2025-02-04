import psycopg2,psycopg2.extras
from utils import *
from postgress_db_connect import get_db_connection


def fetch_and_merge_energy_data(data,start_time,end_time):
    try :
        input_data = fetch_energy_data(data,start_time,end_time)
        conn = get_db_connection()
        cursor = conn.cursor()
    # Prepare data as a list of tuples
        if input_data is None :
            print("The data is not available for location {data}")
            return None
        merge_data = [(record["reportingGroup"], 
                       record["timestamp"],record["value"],record["unit"],
                       record["locationName"])  for record in input_data]
        merge_query = """
            MERGE INTO nuuka_energy_test.energy_usage AS target
            USING (VALUES %s) AS source (reportinggroup, create_dttm, value, unit, location)
            ON target.location = source.location and target.create_dttm = to_timestamp(source.create_dttm,'YYYY-MM-DD')
            WHEN MATCHED THEN
                UPDATE SET
                    value = source.value,
                    unit = source.unit,
                    update_dttm = current_timestamp
            WHEN NOT MATCHED THEN
                INSERT (reportinggroup, create_dttm, value, unit, location, insert_dttm)
                VALUES (source.reportinggroup, to_timestamp(source.create_dttm,'YYYY-MM-DD'),
                  source.value, source.unit, source.location,current_timestamp  );
        """
        
        # Execute merge with batch data
        psycopg2.extras.execute_values(cursor, merge_query, merge_data, template=None, page_size=1000)
        conn.commit()

        print(f"fetch energy data of lcoation {data}")

    except Exception as e:
        #conn.rollback()
        print(f"Error during merge: {e}")

    finally:
        cursor.close()
        conn.close() 