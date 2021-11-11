from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import os
import time
engine = create_engine('your own account').connect()


def generate_query_text(start_time, end_time, individual_str):
    query_text = text("SELECT dw_transaction_id, transaction_dtm, transit_account_id, fare_prod_key, "
                      "media_type_key, transit_day_key, operator_key, "
                      "device_key, payment_types, net_value, "
                      "revenue_or_test "
                      "FROM cta01.ventradb.sale_transaction "
                      "WHERE revenue_or_test = 'REVENUE' " # validate txn
                      "AND net_value > 0 " # validate txn                
                      "AND transit_account_id IN " + individual_str + " " # only sample pax
                      "AND transaction_dtm >= '" + start_time + "' AND transaction_dtm < '" + end_time + "';"
                      )
    # query_text = text("SELECT dw_transaction_id, transaction_dtm, transit_account_id, ride_count, stop_point_key, fare_prod_key, "
    #                   "media_type_key, transit_day_key, operator_key, "
    #                   "device_key, value_changed, payment_types, "
    #                   "revenue_or_test "
    #                   "FROM cta01.ventradb.sale_transaction "
    #                   "WHERE value_changed > 0 " # validate txn
    #                   "AND transit_account_id == numeric(110022426909) "
    #                   "AND transaction_dtm >= '" + start_time + "' AND transaction_dtm < '" + end_time + "';"
    #                   )

    return query_text

# Sep 02 is a holiday
# generate interested pax
individual_choice_Sedgwick = pd.read_csv('data/individual_choice_Sedgwick.csv')
individual_choice_Sedgwick['Case'] = 'Sedgwick'
individual_choice_JFP = pd.read_csv('data/individual_choice_JFP.csv')
individual_choice_JFP['Case'] = 'JFP'
individual_choice = pd.concat([individual_choice_Sedgwick, individual_choice_JFP])


individual_str = "("
all_individual = list(individual_choice['user_id'])
print('num individual',len(all_individual))
count = 0
for user in all_individual:
    count+=1
    if count == len(all_individual):
        individual_str += "" + str(user) + ".0"
    else:
        individual_str += "" + str(user) + ".0" +", "

individual_str += ")"
# WHERE Country IN ('USA', 'UK', 'Japan')


start_date = '2019-01-01' # 2019-01-01
end_date = '2019-12-31' #2019-12-31
tic = time.time()
# file_name_output = 'data/Sale/Sale_txn_' + start_date + '_' + end_date + '.csv'
file_name_output = 'data/Sale/Sale_txn_' + start_date + '_' + end_date + 'two_incidents.csv'
if os.path.exists(file_name_output):
    print(file_name_output, 'exist, skip it \n')
    exit()
else:
    print('process ', file_name_output ,'...')
    query_test = generate_query_text(start_date, end_date, individual_str)
    record = pd.read_sql(query_test, engine)
    print('query time', time.time() - tic)
    record.to_csv(file_name_output, index=False)
    print('total time', time.time() - tic,'\n')

