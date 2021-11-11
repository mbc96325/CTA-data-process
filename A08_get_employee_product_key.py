import pandas as pd

fare_prod_dimension = pd.read_csv('data/fare_product_dimension.csv')
use_col = ['fare_prod_key','fare_prod_name','fare_prod_desc']
fare_prod_dimension = fare_prod_dimension.loc[:,use_col]

employee_fare_prod = fare_prod_dimension.loc[(fare_prod_dimension['fare_prod_name'].str.contains(pat = 'Emp'))|
                                         (fare_prod_dimension['fare_prod_desc'].str.contains(pat = 'Emp'))]

employee_fare_prod.to_csv('data/employee_fare_prod.csv',index=False)
