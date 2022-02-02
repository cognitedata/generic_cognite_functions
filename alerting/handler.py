from itertools import groupby

from cognite.client.data_classes import Event
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils import timestamp_to_ms


def handle(data, client):
    """

    :param data:
    :param client:
    :return:
    """
    dps = client.datapoints.retrieve(
        external_id=data["ts_ext_id"], start=f'{data["scheduled_frequency"]}-ago', end="now"
    ).to_pandas()
    if data["logic"] == "==":
        list_check = dps[data["ts_ext_id"]] == data["threshold"]
        logic_string = "equal to"
    elif data["logic"] == ">":
        list_check = dps[data["ts_ext_id"]] > data["threshold"]
        logic_string = "greater than"
    elif data["logic"] == "<":
        list_check = dps[data["ts_ext_id"]] < data["threshold"]
        logic_string = "lesser than"
    elif data["logic"] == "!=":
        list_check = dps[data["ts_ext_id"]] != data["threshold"]
        logic_string = "not equal to"
    else:
        return "invalid logic"
    data["logic_string"] = logic_string
    groups = [list(g) for _, g in groupby(list_check)]
    res = [0] + [i + 1 for i, (x, y) in enumerate(zip(list_check, list_check[1:])) if x != y]
    if len(groups) < 1:
        return "Threshold not exceeded"
    else:
        pairs = get_date_pairs(groups, res, dps)
        events_resp, all_events = create_events(client, data, pairs)
    if "test" in data:
        test_out = {"logic_result": [x[0] for x in groups], "values": dps.iloc[res][data["ts_ext_id"]].to_list()}
        return events_resp, all_events, test_out
    else:
        return events_resp, all_events, None


def get_date_pairs(groups, res, dps):
    pairs_dates = []
    for i, item in enumerate(groups):
        if item[0] is True:
            if i == 0:
                date_pair = {"start_date": "", "end_date": dps.index[res[i + 1]]}
            elif i == (len(groups) - 1):
                date_pair = {"start_date": dps.index[res[i]], "end_date": ""}
            else:
                date_pair = {"start_date": dps.index[res[i]], "end_date": dps.index[res[i + 1]]}
            pairs_dates.append(date_pair)
    return pairs_dates


def create_events(client, data, pairs_dates):
    ts = client.time_series.retrieve(external_id=data["ts_ext_id"])
    all_events = []
    for i, date_pair in enumerate(pairs_dates):
        if date_pair["start_date"] != "":
            event_string = (
                data["ts_ext_id"]
                + "__"
                + data["logic"]
                + "__"
                + str(data["threshold"])
                + "__"
                + str(timestamp_to_ms(date_pair["start_date"]))
            )
            if data["event_description"] != "auto":
                event_description = data["event_description"]
            else:
                event_description = f"value {data['logic_string']} {data['threshold']}"
            ev = Event(
                external_id=event_string,
                data_set_id=ts.data_set_id,
                asset_ids=[ts.asset_id],
                description=event_description,
                start_time=timestamp_to_ms(date_pair["start_date"]),
                # end_time=date_pair["end_date"].timestamp(),
                metadata=data["metadata"],
                type=data["type"],
            )
            if date_pair["end_date"] != "":
                ev.end_time = timestamp_to_ms(date_pair["end_date"])
            all_events.append(ev)
        else:
            active_at_time = TimestampRange(
                min=timestamp_to_ms(date_pair["end_date"]) - 100, max=timestamp_to_ms(date_pair["end_date"])
            )
            last_events = client.events.list(
                active_at_time=active_at_time,
                asset_ids=[ts.asset_id],
                external_id_prefix=data["ts_ext_id"] + "__" + data["logic"] + "__" + str(data["threshold"]),
                limit=-1,
            ).to_pandas()
            if len(last_events) > 0:
                last_events.sort_values(by=["startTime"], ascending=False, inplace=True)
                ev = client.events.retrieve(last_events.iloc[0]["id"])
                ev.end_time = timestamp_to_ms(date_pair["end_date"])
                client.events.update(ev)
    if "test" in data:
        resp = None
    else:
        try:
            resp = client.events.create(all_events)
        except:
            resp = "error ingesting"
    return resp, all_events
