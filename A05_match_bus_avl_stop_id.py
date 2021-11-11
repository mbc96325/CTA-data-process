'''
We must match AFC (time) with bus avl to get the tap-in station of bus
Bus avl stop_id is not accurate, need to use route+pattern id + stop sequence to get stop id
'''
import pandas as pd
import time
import os

bt_patterndetail = pd.read_csv('data/bt_patterndetail.csv')
bt_pattern = pd.read_csv('data/bt_pattern.csv',low_memory = False)
bt_pattern.loc[:,['route']] = bt_pattern['route'].str.strip()
bt_pattern.loc[:,['pattern']] = bt_pattern['pattern'].str.strip()
bt_stop = pd.read_csv('data/bt_stop.csv')

# avl_date_list = ['2019-09-24','2019-09-23', '2019-09-25']

date_list = [('2019-08-26', '2019-08-27'),('2019-08-27', '2019-08-28'),('2019-08-28', '2019-08-29'), ('2019-08-29', '2019-08-30'),('2019-08-30', '2019-08-31'),
             ('2019-09-03', '2019-09-04'),('2019-09-04', '2019-09-05'),('2019-09-05', '2019-09-06'), ('2019-09-06', '2019-09-07'),
             ('2019-09-09', '2019-09-10'),('2019-09-10', '2019-09-11'),('2019-09-11', '2019-09-12'), ('2019-09-12', '2019-09-13'),('2019-09-13', '2019-09-14'),
             ('2019-09-16', '2019-09-17'),('2019-09-17', '2019-09-18'),('2019-09-18', '2019-09-19'),('2019-09-19', '2019-09-20'),('2019-09-20', '2019-09-21'),
             ('2019-09-23', '2019-09-24'),('2019-09-24', '2019-09-25'),('2019-09-25', '2019-09-26'),('2019-09-26', '2019-09-27'),('2019-09-27', '2019-09-28'),
             ('2019-10-01', '2019-10-02'), ('2019-10-02', '2019-10-03'), ('2019-10-03', '2019-10-04'),
             ('2019-10-08', '2019-10-09'), ('2019-10-09', '2019-10-10'), ('2019-10-10', '2019-10-11'),
             ('2019-10-15', '2019-10-16'), ('2019-10-16', '2019-10-17'), ('2019-10-17', '2019-10-18'),
             ('2019-10-22', '2019-10-23'), ('2019-10-23', '2019-10-24'), ('2019-10-24', '2019-10-25')
             ]

date_list2 = [('2019-01-07', '2019-01-08'),('2019-01-08', '2019-01-09'),('2019-01-09', '2019-01-10'),('2019-01-10', '2019-01-11'), ('2019-01-11', '2019-01-12'),
             ('2019-01-14', '2019-01-15'),('2019-01-15', '2019-01-16'),('2019-01-16', '2019-01-17'),('2019-01-17', '2019-01-18'), ('2019-01-18', '2019-01-19'),
             ('2019-01-22', '2019-01-23'),('2019-01-23', '2019-01-24'),('2019-01-24', '2019-01-25'),('2019-01-25', '2019-01-26'),
             ('2019-01-28', '2019-01-29'),('2019-01-29', '2019-01-30'),('2019-01-30', '2019-01-31'),('2019-01-31', '2019-02-01'), ('2019-02-01', '2019-02-02'),
             ('2019-02-04', '2019-02-05'),('2019-02-05', '2019-02-06'),('2019-02-06', '2019-02-07'),('2019-02-07', '2019-02-08'), ('2019-02-08', '2019-02-09'),
             ('2019-02-11', '2019-02-12'),('2019-02-12', '2019-02-13'),('2019-02-13', '2019-02-14'),('2019-02-14', '2019-02-15'), ('2019-02-15', '2019-02-16'),
             ('2019-02-22', '2019-02-23'),('2019-03-01', '2019-03-02'),('2019-03-08', '2019-03-09'),('2019-03-15', '2019-03-16'), ('2019-03-22', '2019-03-23')
             ]
date_list = date_list + date_list2
avl_date_list = [date[0] for date in date_list]

for date in avl_date_list:
    tic = time.time()
    print('Start date: ', date)
    avl_file = 'data/AVL/bus_avl_' + date + '.csv'
    output_name = avl_file.replace('.csv','with_stop_info.csv')
    # if os.path.exists(output_name):
    #     print(output_name, 'exist, skip it \n')
    #     continue
    bus_avl = pd.read_csv(avl_file, low_memory=False)
    bus_avl = bus_avl.dropna(subset=['route_id', 'pattern'])
    # The three values you should care about are 3 (service stop, door open), 4 (unserviced stop, center of 150ft radius
    # geofence), and 5 (unknown stop, door opens outside of geofence) Unknown stop could be boarding alighting or e.g. stopping for a rail crossing
    bus_avl = bus_avl.loc[bus_avl['event_type'].isin([3,4,5])]

    # delete space
    bus_avl.loc[:, ['route_id']] = bus_avl['route_id'].str.strip()
    bus_avl.loc[:, ['pattern']] = bus_avl['pattern'].str.strip()
    bus_avl_new = bus_avl.merge(bt_pattern[['bt_ver','route','pattern','patternid','direction']], left_on = ['bustools_ver_id','route_id','pattern'],
                       right_on = ['bt_ver','route','pattern'], how = 'inner')
    bus_avl_new = bus_avl_new.merge(bt_patterndetail[['bt_ver','patternid','stopsortorder','geoid']], left_on = ['bt_ver','patternid','stop_sequence'],
                       right_on = ['bt_ver','patternid','stopsortorder'], how = 'inner')
    bus_avl_new = bus_avl_new.merge(bt_stop[['bt_ver','tageoid','geoid','geodescription']], left_on = ['bt_ver','geoid'],
                       right_on = ['bt_ver','geoid'], how = 'inner')
    bus_avl_new = bus_avl_new.sort_values(['bus_id','route_id','pattern','run_id','trip_id','direction','event_time'])
    bus_avl_new['event_timestamp'] = bus_avl_new['event_time'].apply(
        lambda x: int(x.split(' ')[1].split(':')[0]) * 3600 + int(x.split(' ')[1].split(':')[1]) * 60
                  + int(x.split(' ')[1].split(':')[2]))
    output_col = ['bus_id','event_type','event_timestamp','route_id','pattern','run_id','trip_id','direction','bt_ver','stop_sequence','geoid','dwell_time',
                  'latitude','longitude','passenger_load','heading']
    # USE geoid instead, there are several tageoid with str, which is not easy to recognize
    # bus_avl_new = bus_avl_new.sort_values(['geoid'],ascending=False)
    bus_avl_new[output_col].to_csv(output_name,index=False)
    print('End date:', date)
    print('Total time:', round(time.time() - tic, 2), 'sec \n')
