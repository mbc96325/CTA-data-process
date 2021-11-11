from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import os
engine = create_engine('your own account').connect()


# query_text = text("SELECT * FROM "
#                   "id_translation.ventra_facility_to_planning;")

query_text = text("SELECT * FROM "
                  "cta01.schedule_dimension.schd_rail_trips "
                  "WHERE version = 58;")


file_name_output = 'data/schd_rail_trips_version_58.csv'
if os.path.exists(file_name_output):
    print(file_name_output, 'exist, skip it')
else:
    print('process ', file_name_output ,'...')
    query_test = query_text
    record = pd.read_sql(query_test, engine)
    record.to_csv(file_name_output, index=False)


