'''
We must match AFC (time) with bus avl to get the tap-in station of bus
Bus avl stop_id is not accurate, need to use route+pattern id + stop sequence to get stop id
'''
import pandas as pd
import time
import os
pd.options.mode.chained_assignment = None  # default='warn'

def process_multi_tap_in_to_flow(data, col):
    idx = data.index.duplicated(keep='first')
    multiple_tap_in = data.loc[idx].reset_index(drop=False)
    multiple_tap_in['flow'] = 1
    multiple_tap_in['flow'] = multiple_tap_in.groupby(col)['flow'].transform(sum)
    multiple_tap_in = multiple_tap_in.set_index(col)
    multiple_tap_in = multiple_tap_in.loc[~multiple_tap_in.index.duplicated(keep='first')]
    multiple_tap_in['flow'] = multiple_tap_in['flow'].fillna(1)
    data_group_passenger = data.loc[~idx]
    data_group_passenger['flow'] = 1
    data_group_passenger.loc[multiple_tap_in.index,['flow']] += multiple_tap_in.loc[:,['flow']]
    data_group_passenger = data_group_passenger.reset_index(drop=False)
    # test = data_group_passenger.loc[data_group_passenger['flow'].isna()]
    # test2 = multiple_tap_in.loc[multiple_tap_in['flow'].isna()]
    # if len(test)>0:
    #     a=1
    return data_group_passenger

def group_merge(g, bus_avl, window, time_interval_for_group):
    #tic = time.time()
    time_interval_id = g['titg'].iloc[0]
    # print('time_interval_id',time_interval_id)
    bus_avl_temp = bus_avl.loc[(bus_avl['event_timestamp']>= time_interval_id - window) &
                              (bus_avl['event_timestamp']<= (time_interval_id + time_interval_for_group) + window)]
    g = g.merge(bus_avl_temp,on='bus_id',how='left')
    g['time_diff'] = g['event_timestamp'] - g['txn_timestamp']
    g['time_diff'] = g['time_diff'].abs()
    g = g.loc[g.time_diff < window]
    # choose the smallest time_diff
    g = g.sort_values(['time_diff'])
    g = g.groupby(['dw_transaction_id']).first().reset_index()
    # identify group passenger bus
    g = g.sort_values(['user_id','txn_timestamp'])
    g = g.set_index(['user_id','geoid','bus_id','trip_id']) # if all these are same, these passengers must be in the same bus-tap-in.
    col = ['user_id','geoid','bus_id','trip_id']
    g_group_passenger = process_multi_tap_in_to_flow(g, col)
    #temp = g_group_passenger.loc[g_group_passenger['flow']>=2]
    #a=1
    #print(len(g))
    #a=1
    #print('end time', time.time() - tic)
    return g_group_passenger

# no need stop points key, use gtfs id intead
# stop_points_key = pd.read_csv('data/stop_point_dimension.csv')
# stop_points_key_rail = stop_points_key.loc[stop_points_key['operator_name']=='CTA Rail']

ventra_facility_to_planning = pd.read_csv('data/ventra_facility_to_planning.csv')
gtfs_stops = pd.read_csv('../Data/gtfs/stops.txt',sep = ',')
schd_rail_stops = pd.read_csv('data/schd_rail_stops.csv')
schd_rail_stops = schd_rail_stops.loc[:,['station_id','map_id']].drop_duplicates()


avl_date_list = ['2019-08-26','2019-08-27', '2019-08-28', '2019-08-29','2019-08-30',
                 '2019-09-03','2019-09-04', '2019-09-05', '2019-09-06',
                 '2019-09-09','2019-09-10', '2019-09-11', '2019-09-12','2019-09-13',
                 '2019-09-16', '2019-09-17','2019-09-18', '2019-09-19','2019-09-20',
                 '2019-09-23', '2019-09-25', '2019-09-26', '2019-09-27',
                 '2019-10-01', '2019-10-02', '2019-10-03',
                 '2019-10-08', '2019-10-09', '2019-10-10',
                 '2019-10-15', '2019-10-16', '2019-10-17',
                 '2019-10-22', '2019-10-23', '2019-10-24']

# avl_date_list = ['2019-02-01']

date_list = [('2019-01-07', '2019-01-08'),('2019-01-08', '2019-01-09'),('2019-01-09', '2019-01-10'),('2019-01-10', '2019-01-11'), ('2019-01-11', '2019-01-12'),
             ('2019-01-14', '2019-01-15'),('2019-01-15', '2019-01-16'),('2019-01-16', '2019-01-17'),('2019-01-17', '2019-01-18'), ('2019-01-18', '2019-01-19'),
             ('2019-01-22', '2019-01-23'),('2019-01-23', '2019-01-24'),('2019-01-24', '2019-01-25'),('2019-01-25', '2019-01-26'),
             ('2019-01-28', '2019-01-29'),('2019-01-29', '2019-01-30'),('2019-02-01', '2019-02-02'),
             ('2019-02-04', '2019-02-05'),('2019-02-05', '2019-02-06'),('2019-02-06', '2019-02-07'),('2019-02-07', '2019-02-08'), ('2019-02-08', '2019-02-09'),
             ('2019-02-11', '2019-02-12'),('2019-02-12', '2019-02-13'),('2019-02-13', '2019-02-14'),('2019-02-14', '2019-02-15'), ('2019-02-15', '2019-02-16'),
             ('2019-02-22', '2019-02-23'),('2019-03-01', '2019-03-02'),('2019-03-08', '2019-03-09'),('2019-03-15', '2019-03-16'), ('2019-03-22', '2019-03-23')
             ]

avl_date_list += [date[0] for date in date_list]


for date in avl_date_list:
    afc_file = 'data/AFC/AFC_' + date + '.csv'
    out_put_file_name = afc_file.replace('.csv', '_matched.csv')
    # if os.path.exists(out_put_file_name):
    #     print(out_put_file_name, 'exist, skip it \n')
    #     continue

    tic = time.time()
    print('Start date: ', date)
    avl_file = 'data/AVL/bus_avl_' + date + 'with_stop_info.csv'
    bus_avl = pd.read_csv(avl_file, low_memory=False)
    bus_avl = bus_avl.dropna(subset=['route_id', 'pattern'])

    afc = pd.read_csv(afc_file, low_memory=False)
    afc = afc.loc[(afc['operator_key']==1) | (afc['operator_key']==2)] # only consider CTA rail and bus

    # create a new user ID based on serial_nbr and transit_account_id
    afc['user_id'] = afc['transit_account_id']
    afc.loc[afc['transit_account_id'].isna(), 'user_id'] = afc.loc[
        afc['transit_account_id'].isna(), 'serial_nbr']

    afc.loc[:,'dw_transaction_id'] = afc.index + 1 # simplify and accelerate

    afc['txn_timestamp'] = afc['transaction_dtm'].apply(
        lambda x: int(x.split(' ')[1].split(':')[0]) * 3600 +
                  int(x.split(' ')[1].split(':')[1]) * 60 + int(x.split(' ')[1].split(':')[2]))
    print('afc raw:', len(afc))

    afc_bus = afc.loc[~afc['bus_id'].isna()]
    afc_rail = afc.loc[afc['bus_id'].isna()]

    print('afc bus raw:', len(afc_bus))
    print('afc rail raw:', len(afc_rail))
    # change dw_transaction_id to number

    useful_col_afc = ['dw_transaction_id', 'txn_timestamp', 'user_id', 'bus_id', 'fare_prod_key',
                      'transfer_sequence_nbr','calculated_fare']
    useful_col_avl = ['bus_id', 'event_timestamp', 'geoid','trip_id','route_id']

    # to large to directly merge, use bus_id to group and do merge in each group
    window = 10 * 60  # 10 minutes windows for matching
    time_interval_for_group = 2 * 3600  # 2 hours

    # seperate by time
    afc_bus['time_interval_to_group'] = afc_bus['txn_timestamp'] // time_interval_for_group * time_interval_for_group
    bus_avl['time_interval_to_group'] = bus_avl['event_timestamp'] // time_interval_for_group * time_interval_for_group
    afc_bus['titg'] = afc_bus['time_interval_to_group']

    tic2 = time.time()
    afc_bus_new = afc_bus[useful_col_afc + ['time_interval_to_group', 'titg']].groupby(
        ['time_interval_to_group']).apply(group_merge, bus_avl[useful_col_avl], window, time_interval_for_group)
    afc_bus_new = afc_bus_new.reset_index(drop=True)
    print('merge time', time.time() - tic2)


    # get rail stop info
    tic2 = time.time()
    afc_rail_new = afc_rail[useful_col_afc + ['facility_id','multi_ride_id']].merge(
        ventra_facility_to_planning[['facility_id', 'station_id','facility_name']],
        on=['facility_id'], how='left')
    afc_rail_new = afc_rail_new.merge(schd_rail_stops[['station_id','map_id']], on =['station_id'],
                                      how = 'left')

    # afc_rail_new = afc_rail[useful_col_afc + ['stop_point_key','multi_ride_id']].merge(
    #     stop_points_key_rail[['stop_point_key', 'stop_point_name']],
    #     on=['stop_point_key'], how='left')

    ############check na
    afc_rail_na = afc_rail_new.loc[afc_rail_new['map_id'].isna()]

    # combine afc_bus and afc rail, output final afc
    print('rail time', time.time() - tic2)

    afc_rail_new['operator'] = 'rail'
    afc_bus_new['operator'] = 'bus'
    afc_bus_new['tap_in'] = afc_bus_new['geoid']
    afc_bus_new['bus_route_id'] = afc_bus_new['route_id']
    afc_rail_new['tap_in'] = afc_rail_new['map_id']

    ########## Identify group passengers (tap-in with the same account in same time)
    # bus has been processed when matching with AVL
    # rail: For simplication, just trust multi_ride_id (this is what ODX use).
    rail_multi = afc_rail_new.loc[~afc_rail_new['multi_ride_id'].isna()]
    col_group = ['user_id','tap_in','multi_ride_id']
    rail_multi = rail_multi.set_index(col_group)
    rail_group = process_multi_tap_in_to_flow(rail_multi, col_group)

    rail_single = afc_rail_new.loc[afc_rail_new['multi_ride_id'].isna()]
    rail_single['flow'] = 1
    afc_rail_group = pd.concat([rail_single, rail_group],sort=False)
    # temp = afc_rail_group.loc[afc_rail_group['flow']>=2]
    ##############
    afc_rail_group['bus_route_id'] = -1
    out_put_afc_col = ['dw_transaction_id', 'txn_timestamp', 'user_id', 'operator', 'tap_in', 'fare_prod_key',
                       'transfer_sequence_nbr','calculated_fare','bus_route_id','flow']
    afc_new = pd.concat([afc_rail_group[out_put_afc_col], afc_bus_new[out_put_afc_col]],sort=False).sort_values(
        ['user_id','txn_timestamp'],ascending = [False,True])
    # same station
    afc_new = afc_new.drop_duplicates()
    print('afc new:', len(afc_new))
    afc_new = afc_new.dropna()
    print('afc new no na:', len(afc_new))
    ########## data type ##########
    afc_new['tap_in'] = afc_new['tap_in'].apply(int)
    afc_new['dw_transaction_id'] = afc_new['dw_transaction_id'].apply(int)
    afc_new['txn_timestamp'] = afc_new['txn_timestamp'].apply(int)
    #print(type(afc_new['tap_in'].iloc[0]))
    afc_new['user_id'] = afc_new['user_id'].apply(int)
    afc_new['fare_prod_key'] = afc_new['fare_prod_key'].apply(int)
    afc_new['transfer_sequence_nbr'] = afc_new['transfer_sequence_nbr'].apply(int)
    afc_new['calculated_fare'] = afc_new['calculated_fare'].apply(int)
    afc_new['bus_route_id'] = afc_new['bus_route_id'].apply(str)
    afc_new['flow'] = afc_new['flow'].apply(int)

    ##########Match with fare product
    # fare_product_dimension = pd.read_csv('data/fare_product_dimension.csv')
    # afc_new = afc_new.merge(fare_product_dimension[['fare_prod_key','fare_prod_name']],on = ['fare_prod_key'], how = 'left')
    # afc_new = afc_new.drop(columns = ['fare_prod_key'])


    ############check na
    # afc_bus_new_na = afc_bus_new.loc[afc_bus_new['flow'].isna()]
    afc_new_na = afc_new.loc[afc_new.isnull().any(axis=1)]

    afc_new.to_csv(out_put_file_name, index=False)

    #####################

    #####################
    print('End date:', date)
    print('Total time:', round(time.time() - tic, 2), 'sec\n')
