from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import os
import time

engine = create_engine('your own account').connect()


def generate_query_text(start_time, end_time):
    query_text1 = text("SELECT event_type, run_id, scada, locationdesc, line_id, dir_id, event_time, track_id, qt2_trackid, headway, direction, action "
                      "FROM cta01.avas_spectrum.qt2_trainevent "
                      "WHERE event_time >= '" + start_time + "' AND event_time <'" + end_time + "';"
                      )

    query_text2 = text("SELECT event_datetime, qt2_trackid, track_desc, status, dir_id, dwell_arrtodep, hdw_deptoarr, hdw_arrtoarr, stop_id, stopname, public_name, schd_ver, sched_avghdw, sched_cnt, sched_maxhdw, sched_minhdw "
                      "FROM cta01.avas_spectrum.qt2_trackhdw "
                      "WHERE event_datetime >= '" + start_time + "' AND event_datetime <'" + end_time + "';"
                      )


    return query_text1, query_text2

# date_list = [('2019-09-24', '2019-09-25')]
# date_list = [('2019-08-26', '2019-08-27'),('2019-08-27', '2019-08-28'),('2019-08-28', '2019-08-29'), ('2019-08-29', '2019-08-30'),('2019-08-30', '2019-08-31'),
#              ('2019-09-03', '2019-09-04'),('2019-09-04', '2019-09-05'),('2019-09-05', '2019-09-06'), ('2019-09-06', '2019-09-07'),
#              ('2019-09-09', '2019-09-10'),('2019-09-10', '2019-09-11'),('2019-09-11', '2019-09-12'), ('2019-09-12', '2019-09-13'),('2019-09-13', '2019-09-14'),
#              ('2019-09-16', '2019-09-17'),('2019-09-17', '2019-09-18'),('2019-09-18', '2019-09-19'),('2019-09-19', '2019-09-20'),('2019-09-20', '2019-09-21'),
#              ('2019-09-23', '2019-09-24'),('2019-09-24', '2019-09-25'),('2019-09-25', '2019-09-26'),('2019-09-26', '2019-09-27'),('2019-09-27', '2019-09-28'),
#              ('2019-10-01', '2019-10-02'),('2019-10-02', '2019-10-03'),('2019-10-03', '2019-10-04'),
#              ('2019-10-08', '2019-10-09'),('2019-10-09', '2019-10-10'),('2019-10-10', '2019-10-11'),
#              ('2019-10-15', '2019-10-16'),('2019-10-16', '2019-10-17'),('2019-10-17', '2019-10-18'),
#              ('2019-10-22', '2019-10-23'),('2019-10-23', '2019-10-24'),('2019-10-24', '2019-10-25')]

date_list = [('2019-01-07', '2019-01-08'),('2019-01-08', '2019-01-09'),('2019-01-09', '2019-01-10'),('2019-01-10', '2019-01-11'), ('2019-01-11', '2019-01-12'),
             ('2019-01-14', '2019-01-15'),('2019-01-15', '2019-01-16'),('2019-01-16', '2019-01-17'),('2019-01-17', '2019-01-18'), ('2019-01-18', '2019-01-19'),
             ('2019-01-22', '2019-01-23'),('2019-01-23', '2019-01-24'),('2019-01-24', '2019-01-25'),('2019-01-25', '2019-01-26'),
             ('2019-01-28', '2019-01-29'),('2019-01-29', '2019-01-30'),('2019-02-01', '2019-02-02'),
             ('2019-02-04', '2019-02-05'),('2019-02-05', '2019-02-06'),('2019-02-06', '2019-02-07'),('2019-02-07', '2019-02-08'), ('2019-02-08', '2019-02-09'),
             ('2019-02-11', '2019-02-12'),('2019-02-12', '2019-02-13'),('2019-02-13', '2019-02-14'),('2019-02-14', '2019-02-15'), ('2019-02-15', '2019-02-16'),
             ('2019-02-22', '2019-02-23'),('2019-03-01', '2019-03-02'),('2019-03-08', '2019-03-09'),('2019-03-15', '2019-03-16'), ('2019-03-22', '2019-03-23')
             ]

for date in date_list:
    tic = time.time()
    start_date = date[0]
    end_data = date[1]
    file_name_output1 = 'data/AVL/train_tracker_event' + start_date + '.csv'
    file_name_output2 = 'data/AVL/train_tracker_headway' + start_date + '.csv'
    if os.path.exists(file_name_output1):
        print(file_name_output1, 'exist, skip it','\n')
        continue
    else:
        print('process ', file_name_output1 ,'...')
        query_test1,  query_test2 = generate_query_text(start_date, end_data)
        record1 = pd.read_sql(query_test1, engine)
        record2 = pd.read_sql(query_test2, engine)
        print('query time', time.time() - tic)
        record1.to_csv(file_name_output1, index=False)
        record2.to_csv(file_name_output2, index=False)

    print('total time', time.time() - tic,'\n')
