from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import os

engine = create_engine('your own account').connect()


def generate_query_text(start_date, end_date):
    query_text = text(
        "SELECT timec, timerc, line, lineall, station, datec, direction, nature, naturefinal, drqbe, run, terminal, timert, delay, details "
        "FROM cta01.cpc.rrecord "
        "WHERE datec >= '" + start_date + "' AND datec <= '" + end_date + "';"
        )
    # query_text = text("SELECT * "
    #                   "FROM cta01.cpc.rrecord "
    #                   "WHERE datec >= '" + start_date + "' AND datec <= '" + end_date + "';"
    #                   )

    return query_text


month_list = [('2019-01-01', '2019-01-31'), ('2019-02-01', '2019-02-28'), ('2019-03-01', '2019-03-31'),
              ('2019-04-01', '2019-04-30'),
              ('2019-05-01', '2019-05-31'), ('2019-06-01', '2019-06-30'), ('2019-07-01', '2019-07-31'),
              ('2019-08-01', '2019-08-31'),
              ('2019-09-01', '2019-09-30'), ('2019-10-01', '2019-10-31'), ('2019-11-01', '2019-11-30'),
              ('2019-12-01', '2019-12-31')]

month_list2 = [(month[0].replace('2019', '2018'), month[1].replace('2019', '2018')) for month in month_list]

for month in month_list2:
    start_date = month[0]
    end_date = month[1]
    file_name_output = 'data/Control_center/record_' + start_date + '_' + end_date + '.csv'
    if os.path.exists(file_name_output):
        print(file_name_output, 'exist, skip it')
    else:
        print('process ', file_name_output, '...')
        query_test = generate_query_text(start_date, end_date)
        record = pd.read_sql(query_test, engine)
        record.to_csv(file_name_output, index=False)
