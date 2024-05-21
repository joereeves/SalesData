import common_utils as com
import config as conf

import pandas as pd

##Parameters


##========================================================
##----------------------Store Data------------------------

#Fetching Store data
store_df = pd.read_csv("./Data/Store_data.csv", header='infer')

##---------Incorporating Weather Data----------------------

dist_city = store_df["city"].unique()

final_weather_df = pd.DataFrame()
for city in dist_city:
    weather_api_url = conf.config_dic["weather_api_url"] + "{}&q={}".format(conf.config_dic["weather_api_key"], city)
    weather_info = com.get_weather_api(
        conf.config_dic["weather_api_url"] + "{}&q={}".format(conf.config_dic["weather_api_key"], city), city)
    weather_df = pd.DataFrame(weather_info)
    final_weather_df = pd.concat([final_weather_df, weather_df], axis=0)

final_store_df = pd.merge(store_df, final_weather_df, on="city")

#Truncate and inserting Store data into Sales_data.stg_store
com.truncate_table(conf.config_dic["store_table"])
com.insert_into_table(conf.config_dic["store_table"], final_store_df)

##========================================================
##----------------------User Data------------------------

#Fetching User data
customer_df = com.get_data_api(conf.config_dic["user_url"])
customer_df = customer_df.rename(columns={'id': 'customer_id', 'username': 'user_name',
                                          'address.street': 'addr_street', 'address.suite': 'addr_suite',
                                          'address.city': 'city',
                                          'address.zipcode': 'zip_code', 'address.geo.lat': 'geo_lat',
                                          'address.geo.lng': 'geo_lng',
                                          'company.name': 'company_name', 'company.catchPhrase': 'company_catch_phrase',
                                          'company.bs': 'company_bs'})

#DQ check on Customer_id column
customer_df = com.remove_nonnum_null(customer_df,
                                     "customer_id")  # removing null and non numeric values from df in customer_id


#Truncate and inserting customer data into Sales_data.stg_customer
com.truncate_table(conf.config_dic["customer_table"])
com.insert_into_table(conf.config_dic["customer_table"], customer_df)

##========================================================
##----------------------Sales Data------------------------

##reading Sales CSV file into Pandas Dataframe
sales_df = pd.read_csv("./Data/AIQ - Data Engineer Assignment - Sales data.csv", header='infer')

# DataQuality check on
# 1. Order_id, product_id, quantity and price should be non numeric and non null
num_check_df = com.remove_nonnum_null(sales_df, "order_id")  # removing null and non numeric values from df in order_id
num_check_df = com.remove_nonnum_null(num_check_df,
                                      "product_id")  # removing null and non numeric values from df in product_id
num_check_df = com.remove_nonnum_null(num_check_df,
                                      "quantity")  # removing null and non numeric values from df in quantity
num_check_df = com.remove_nonnum_null(num_check_df, "price")  # removing null and non numeric values from df in price

# 2. Price should not be negative
num_check_df = com.remove_negative_num(num_check_df,
                                       "price")  # removing null and non numeric values from df in order_id

#getting the count of data storing in a variable
data_count = num_check_df.count().iloc[0]

#Adding Store Id to the Sales data
num_check_df['store_id'] = pd.Series(range(1, 6)).sample(int(data_count), replace=True).array

#Truncate and inserting sales data into Sales_data.stg_sales
com.truncate_table(conf.config_dic["sales_table"])
com.insert_into_table(conf.config_dic["sales_table"], num_check_df)

#================================================
df_cal = pd.read_csv("./Data/calendar_dim.csv", header='infer')

#Truncate and inserting calendar data into Sales_data.dim_calendar
com.truncate_table(conf.config_dic["cal_table"])
com.insert_into_table(conf.config_dic["cal_table"], df_cal)


