## Alerting Function


# Arguments dictionary

{
    "start_time": "4h-ago",
    "end_time": "now",
    "agg": "average",
    "gran": "1m",
    "input_timeseries": "Time_series_1",
    "filter_type": "moving_average",
    "filter_order": 121
  }
  
# Function description

This function generates filtered output (timeseries) of the given timeseries.
 

# Explanation of the data dictionary

Step 1: Pulls the data for "Time_series_1" last 4 hours until now with average as aggregate function and granularity of 1m. 
Step 2: Applies "moving_average" filter_order with the order 121 (which is 121 minutes (~2 hour window) since granularity is 1m, you can play around these 2 parameters to filter the data for the duration you want)




#Owner: 
karan.rajagopalan@cognite.com
