import pandas as pd
import time
import os
pd.options.mode.chained_assignment = None  # default='warn'

line_name_id = pd.read_csv('data/line_name_id.csv')
schd_rail = pd.read_csv('data/schd_rail_stops.csv')
date_list = ['2019-09-24']
# date_list = ['2019-08-26','2019-08-27', '2019-08-28', '2019-08-29','2019-08-30',
#                    '2019-09-03','2019-09-04', '2019-09-05', '2019-09-06',
#                    '2019-09-09','2019-09-10', '2019-09-11', '2019-09-12','2019-09-13',
#                    '2019-09-16', '2019-09-17','2019-09-18', '2019-09-19','2019-09-20',
#                    '2019-09-23', '2019-09-25', '2019-09-26', '2019-09-27']
for date in date_list:
    print('====================')
    print('current date', date)
    train_tracker = pd.read_csv('data/AVL/train_tracker_event'+ date + '.csv')
    train_tracker = train_tracker.dropna()


    train_tracker = train_tracker.loc[train_tracker['action'] == 'MOVE']

    # idx = train_tracker['locationdesc'].str.contains(pat = 'arriving')
    # train_tracker_arr = train_tracker.loc[idx]
    # train_tracker_dep = train_tracker.loc[train_tracker['locationdesc'].str.contains(pat = 'leaving')]
    # train_avl = pd.concat([train_tracker_arr, train_tracker_dep],sort=False)
    # train_avl = train_avl.sort_values(['line_id','dir_id','run_id','event_time'])
    #
    # train_avl = train_avl.merge(line_name_id[['line_id','line_name']],on = ['line_id'])

    train_tracker = train_tracker.merge(schd_rail, on = ['qt2_trackid'], how = 'left')
    train_tracker = train_tracker.sort_values(['line_id','run_id','direction_id','event_time'])
    train_tracker_no_na = train_tracker.dropna()
    train_tracker_no_na['event_timestamp'] = train_tracker_no_na['event_time'].apply(
        lambda x: int(x.split(' ')[1].split(':')[0]) * 3600 + int(x.split(' ')[1].split(':')[1]) * 60
                  + int(x.split(' ')[1].split(':')[2]))
    use_col = ['line_id','direction_id','dir_id','run_id','event_timestamp','headway','stop_id','track_id','map_id','stopname','public_name']
    train_avl = train_tracker_no_na.loc[:,use_col]
    output_name = 'data/AVL/train_avl_' + date + '.csv'
    train_avl.to_csv(output_name,index=False)


