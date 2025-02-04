import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed

from ingest_energy_consumption import fetch_and_insert_energy_data
from merge_energy_consumption import fetch_and_merge_energy_data
from utils import *



if __name__=="__main__" :
    print("hello")
    last_run_date =get_last_successful_run_date()
    if last_run_date :
      
      start_time=last_run_date
      start_time = datetime.datetime.strptime('2025-02-02','%Y-%m-%d').date()
      end_time  = datetime.date.today()
      print(start_time)
    else: 
      print("else loop")
      start_time = datetime.datetime.strptime('2025-01-01','%Y-%m-%d').date()
      end_time = datetime.date.today()
   
    # Using ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit tasks to the executor
        if last_run_date :
           future_to_property = {
            executor.submit(fetch_and_merge_energy_data, prop.get("propertyName"),start_time,end_time): prop
            for prop in get_properties()}
        else :
           future_to_property = {
            executor.submit(fetch_and_insert_energy_data, prop.get("propertyName"),start_time,end_time): prop
            for prop in get_properties()}
             

        # Collect results as they complete
        for future in as_completed(future_to_property):
            prop = future_to_property[future]
            try:
                result = future.result()
            except Exception as e:
                print(f"Error processing {prop['propertyName']}: {e}") 
                insert_audit_log('FAILED') 
    
    insert_audit_log('SUCCESS')

