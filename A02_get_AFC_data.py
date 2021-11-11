from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import os
import time
engine = create_engine('your own account').connect()


def generate_query_text(start_time, end_time):
    query_text = text("SELECT dw_transaction_id, transaction_dtm, transit_account_id, ride_count, stop_point_key, fare_prod_key, "
                      "media_type_key, transit_day_key, operator_key, "
                      "device_key, ride_type_key, txn_status_key, multi_ride_id, "
                      "calculated_fare, transfer_sequence_nbr, journey_start_dtm, "
                      "serial_nbr, fare_due, pass_cost, pass_id, pass_use_count, "
                      "facility_id, bus_id "
                      "FROM cta01.ventradb.use_transaction "
                      "WHERE ((txn_status_key = 0) OR (txn_status_key = 33)) " # validate txn
                      "AND ride_count >= 1 " # validate txn
                      "AND transaction_dtm >= '" + start_time + "' AND transaction_dtm < '" + end_time + "';"
                      )

    return query_text

# Sep 02 is a holiday
# date_list = [('2019-08-26', '2019-08-27'),('2019-08-27', '2019-08-28'),('2019-08-28', '2019-08-29'), ('2019-08-29', '2019-08-30'),('2019-08-30', '2019-08-31'),
#              ('2019-09-03', '2019-09-04'),('2019-09-04', '2019-09-05'),('2019-09-05', '2019-09-06'), ('2019-09-06', '2019-09-07'),
#              ('2019-09-09', '2019-09-10'),('2019-09-10', '2019-09-11'),('2019-09-11', '2019-09-12'), ('2019-09-12', '2019-09-13'),('2019-09-13', '2019-09-14'),
#              ('2019-09-16', '2019-09-17'),('2019-09-17', '2019-09-18'),('2019-09-18', '2019-09-19'),('2019-09-19', '2019-09-20'),('2019-09-20', '2019-09-21'),
#              ('2019-09-23', '2019-09-24'),('2019-09-24', '2019-09-25'),('2019-09-25', '2019-09-26'),('2019-09-26', '2019-09-27'),('2019-09-27', '2019-09-28'),
#              ('2019-10-01', '2019-10-02'), ('2019-10-02', '2019-10-03'), ('2019-10-03', '2019-10-04'),
#              ('2019-10-08', '2019-10-09'), ('2019-10-09', '2019-10-10'), ('2019-10-10', '2019-10-11'),
#              ('2019-10-15', '2019-10-16'), ('2019-10-16', '2019-10-17'), ('2019-10-17', '2019-10-18'),
#              ('2019-10-22', '2019-10-23'), ('2019-10-23', '2019-10-24'), ('2019-10-24', '2019-10-25')
#              ]

# Jan 21 holiday 30 31 is abnormal
date_list = [('2019-01-07', '2019-01-08'),('2019-01-08', '2019-01-09'),('2019-01-09', '2019-01-10'),('2019-01-10', '2019-01-11'), ('2019-01-11', '2019-01-12'),
             ('2019-01-14', '2019-01-15'),('2019-01-15', '2019-01-16'),('2019-01-16', '2019-01-17'),('2019-01-17', '2019-01-18'), ('2019-01-18', '2019-01-19'),
             ('2019-01-22', '2019-01-23'),('2019-01-23', '2019-01-24'),('2019-01-24', '2019-01-25'),('2019-01-25', '2019-01-26'),
             ('2019-01-28', '2019-01-29'),('2019-01-29', '2019-01-30'),('2019-02-01', '2019-02-02'),
             ('2019-02-04', '2019-02-05'),('2019-02-05', '2019-02-06'),('2019-02-06', '2019-02-07'),('2019-02-07', '2019-02-08'), ('2019-02-08', '2019-02-09'),
             ('2019-02-11', '2019-02-12'),('2019-02-12', '2019-02-13'),('2019-02-13', '2019-02-14'),('2019-02-14', '2019-02-15'), ('2019-02-15', '2019-02-16'),
             ('2019-02-22', '2019-02-23'),('2019-03-01', '2019-03-02'),('2019-03-08', '2019-03-09'),('2019-03-15', '2019-03-16'), ('2019-03-22', '2019-03-23')
             ]

for date in date_list:
    start_date = date[0]
    end_data = date[1]
    tic = time.time()
    file_name_output = 'data/AFC/AFC_' + start_date + '.csv'
    if os.path.exists(file_name_output):
        print(file_name_output, 'exist, skip it \n')
        continue
    else:
        print('process ', file_name_output ,'...')
        query_test = generate_query_text(start_date, end_data)
        record = pd.read_sql(query_test, engine)
        print('query time', date[0], time.time() - tic)
        record.to_csv(file_name_output, index=False)
        print('total time', date[0], time.time() - tic,'\n')

