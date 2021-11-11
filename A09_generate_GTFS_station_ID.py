import pandas as pd

def rail_station():
    data = pd.read_csv('data/GTFS_stations.txt',sep = ',', header  = None)
    data = data.rename(columns = {0:'ID',2:'Name'})
    use_col = ['ID','Name']
    data = data.loc[:,use_col]
    link_info = pd.read_csv('data/link_info.csv')
    station1 = link_info.loc[:,['link_start','route_id','direction_id','link_start_parent']].rename(columns = {'link_start':'station_id','link_start_parent':'tap_in_ID'})
    station2 = link_info.loc[:,['link_end','route_id','direction_id','link_end_parent']].rename(columns = {'link_end':'station_id','link_end_parent':'tap_in_ID'})
    station = station1.append(station2)
    station = station.drop_duplicates()
    station = station.merge(data, left_on = ['tap_in_ID'], right_on = ['ID'])
    schd_rail = pd.read_csv('data/schd_rail_stops.csv')
    len_old = len(station)
    station = station.merge(schd_rail,left_on = ['station_id'],right_on = ['stop_id'])
    len_new = len(station)
    if len_old!=len_new:
        print('missing data, please check')
        exit()
    line_name_id = pd.read_csv('data/line_name_id.csv')
    station = station.merge(line_name_id,left_on = ['route_id'],right_on = ['line_short'])
    station = station.rename(columns = {'station_id_x':'station_id','direction_id_x':'GTFS_direction','direction_id_y':'CTA_schedule_direction','stopname':'CTA_stopname'})
    col_out = ['station_id','tap_in_ID','Name','longitude','latitude','qt2_trackid','line_id','line_name','CTA_schedule_direction','CTA_stopname']
    station['station_id'] = station['station_id'].astype(int)
    station['tap_in_ID'] = station['tap_in_ID'].astype(int)
    data_save = station.loc[:,col_out]
    data_save = data_save.drop_duplicates()
    data_save.to_csv('data/All_rail_stations.csv',index=False)
    a=1

def bus_station():
    data = pd.read_csv('data/bt_stop.csv')
    # bt_pattern = pd.read_csv('data/bt_pattern.csv')
    use_col = ['geoid','geodescription','longitude','latitude','tageoid']
    data = data.loc[:,use_col]
    data = data.rename(columns = {'geoid':'tap_in_ID','geodescription':'Name','tageoid': 'GTFS_id'})
    col_out = ['tap_in_ID','Name','longitude','latitude','GTFS_id']
    data_save = data.loc[:,col_out]
    data_save = data_save.drop_duplicates(['tap_in_ID'])
    data_save.to_csv('data/All_bus_stations.csv',index=False)
    a=1

if __name__ == '__main__':
    bus_station()