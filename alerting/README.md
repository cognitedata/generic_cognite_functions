## Alerting Function


# Arguments dictionary

{
    "event_description": "auto",
    "logic": ">",
    "logic_string": "greater than",
    "metadata": {
      "Info": "Test",
      "Purpose": "Test"
    },
    "scheduled_frequency": "2h",
    "threshold": "40",
    "ts_ext_id": "my_time_series",
    "type": "cognite_functions_alerting"
  }
  
# Function description

This function generates an event when the condition given is satisfied. The function creates and event on the data point when the condtion is satified and closes the event with the timestamp of the datapoint when it is not satified again ( Can look backwards as well, meaning, if the event was created in the previous run of the schedule, it can close it the next run when the condtion is not satisfied)

# Explanation of the data dictionary

if data["ts_ext_id"] > data["threshold"] :
    create an event with the following ({data["metadata"],data["type"], data["logic_string"]})
if data["event_description"] == "auto":
	It generated description automatically based on the logic_string, threshold
else
    It used the string given in this key for event description.
	

#Owner: 
karan.rajagopalan@cognite.com
