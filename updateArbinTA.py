from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import requests
import pandas as pd


def extract_arbin_log(file_path: str) -> tuple[list[str], list[str]]:
    # Extract and clean up timestamps and following messages
    with open(file_path, "r", encoding="windows-1252") as f:
        soup = BeautifulSoup(f, "html.parser")

    timestamps = soup.find_all("font", color="#008000")
    clean_ts = [" ".join(timestamp.text.split()) for timestamp in timestamps]
    dt_str = [ts[1:21] for ts in clean_ts]  # text only, e.g., "2026-02-25, 07:59:13"

    messages = soup.find_all("font", color="#000000")
    ms_str = [msg.text.strip() for msg in messages]

    return dt_str, ms_str


def extract_latest_activities(
        dt_str: list[str],
        ms_str: list[str],
        cutoff: datetime
) -> list[dict]:

    # Search for the test names and channels of "start test" activities within the threshold
    start_activities = [
        (ts, msg) for (ts, msg) in zip(dt_str, ms_str)
        if ("Succeeded to start test" in msg or "Succeeded to resume" in msg)
        and datetime.strptime(ts, "%Y-%m-%d, %H:%M:%S") > cutoff
    ]
    start_tasks = [msg for (ts, msg) in start_activities]
    start_times = [ts for (ts, msg) in start_activities]
    test_names = []
    start_channels = []
    for task in start_tasks:
        match_testname = re.search(r"start test ([\d_]+)", task)  # RegEx
        match_channel = re.search(r"Channel (\d+)", task)  # RegEx
        if match_testname:
            test_names.append(match_testname.group(1))
        else:
            test_names.append("N/A")
        if match_channel:
            start_channels.append(match_channel.group(1))
        else:
            start_channels.append("N/A")
    res1 = [
        {"time": ts,
         "channel": channel,
         "TN": tn,
         "status_update": "start"}
        for (ts, channel, tn) in zip(start_times, start_channels, test_names)
    ]

    # Search for the test names and channels of "stop" activities within the threshold
    stop_activities = [
        (ts, msg) for (ts, msg) in zip(dt_str, ms_str)
        if "Succeeded to stop" in msg and datetime.strptime(ts, "%Y-%m-%d, %H:%M:%S") > cutoff
    ]
    stop_tasks = [msg for (ts, msg) in stop_activities]
    stop_times = [ts for (ts, msg) in stop_activities]
    stop_channels = []
    for task in stop_tasks:
        match_channel = re.search(r"Channel (\d+)", task)  # RegEx
        if match_channel:
            stop_channels.append(match_channel.group(1))
        else:
            stop_channels.append("N/A")
    res2 = [
        {"time": ts,
         "channel": channel,
         "TN": "N/A",
         "status_update": "stop"}
        for (ts, channel) in zip(stop_times, stop_channels)
    ]
    activity_list = res1 + res2
    return activity_list


def get_updates(activity_list: list[dict]) -> tuple[list, list, list]:
    """Get latest activity on each channel."""
    activity_list.sort(key=lambda x: x["time"])  # 'lambda' sort
    channel_history = {}  # dictionary to track the history of each channel

    # Loop through all sorted activities and create a sorted list of activities for each channel
    for activity in activity_list:
        ch = activity["channel"]
        # If we haven't seen this channel yet, create an empty list for it
        if ch not in channel_history:
            channel_history[ch] = []
        # Add the activity to this channel's history
        channel_history[ch].append(activity)

    # Get latest action on each channel
    last_actions = [history[-1] for (ch, history) in channel_history.items()]  # list of dictionaries, row-based
    ch_updates = [row["channel"] for row in last_actions]
    tn_updates = [row["TN"] for row in last_actions]
    st_updates = [row["status_update"] for row in last_actions]
    return ch_updates, tn_updates, st_updates


def fetch_qb_records(tester_name: str, ch_updates: list[str]) -> pd.DataFrame:
    """Fetch QuickBase records that matches the channel ID from the tester log."""
    fid_list = [438, 177, 3, 76]
    full_channel_id = [tester_name + ' _ ' + channel for channel in ch_updates]
    conditions = [f"{{438.EX.'{ch}'}}" for ch in full_channel_id]
    query = " OR ".join(conditions)
    token = "qb_user_token"  # dbrobot user token
    headers = {
        'QB-Realm-Hostname': "https://company.quickbase.com",
        'User-Agent': 'Amprius',
        'Authorization': 'QB-USER-TOKEN ' + token
    }
    body = {
        "from": "table_id",  # Cell Test table
        "select": fid_list,
        "where": query
    }
    r = requests.post(
        'https://api.quickbase.com/v1/records/query',
        headers=headers,
        json=body
    )
    #json_export = (json.dumps(r.json(), indent=4))
    #df = pd.json_normalize(json.loads(json_export)['data'])
    return pd.json_normalize(r.json()['data'])


def calculate_status_changes(df: pd.DataFrame, ch_updates, tn_updates, st_updates) -> list[dict]:
    """Determine status changes from QuickBase to Tester log"""
    if df.empty:
        return []

    # Map columns
    fid_to_name = {3: "Test ID", 76: "Status", 177: "Test Name - Actual", 438: "Test Channel - Channel ID"}
    df.columns = [fid_to_name.get(int(col.split('.')[0]), col) for col in df.columns]

    df["Channel Number"] = df["Test Channel - Channel ID"].apply(
        lambda x: re.search(r" _ (\d+)", str(x)).group(1) if re.search(r" _ (\d+)", str(x)) else "N/A"
    )  # use a lambda to safely get RegEx group(1)

    # Get build a new column "New Status" for the df using Dictionary lookup
    tn_lookup = {tn: status for tn, status in zip(tn_updates, st_updates) if tn != "N/A"}
    ch_lookup = {ch: status for ch, status in zip(ch_updates, st_updates) if ch != "N/A"}
    new_statuses = []
    for index, row in df.iterrows():
        qb_tn = str(row.get("Test Name - Actual", ""))
        qb_ch_num = str(row.get("Channel Number", ""))
        qb_status = row.get("Status")
        # Does the Test Name match? If not, does the Channel match? Otherwise, keep old status.
        # due to priority given to tn_lookup, cannot collapse to 1line of code
        if qb_tn in tn_lookup:
            new_statuses.append(tn_lookup[qb_tn])
        elif qb_ch_num in ch_lookup:
            new_statuses.append(ch_lookup[qb_ch_num])
        else:
            new_statuses.append(qb_status)
    df["New Status"] = new_statuses

    # Build records_to_import in the QB required format
    # records_to_import = list of records, each record follows the format
    # {fid1[str]: { "value": value1[str] }, fid2[str]: {"value": value2[str]} }
    records_to_update = []
    for index, row in df.iterrows():
        if row["Status"] != row["New Status"]:
            records_to_update.append({
                "3": {"value": row["Test ID"]},
                "76": {"value": row["New Status"]}
            })
    return records_to_update


def import_to_qb(records_to_update: list[dict]):
    # Prepare the API Request
    url = f"https://api.quickbase.com/v1/records"
    token = "qb_user_token"   # dbrobot user token
    headers = {
        "QB-Realm-Hostname": "https://company.quickbase.com",
        "Authorization": 'QB-USER-TOKEN ' + token,
        "Content-Type": "application/json"
    }
    payload = {
        "to": "table_id",
        "data": records_to_update,
        "mergeFieldId": '3'
    }
    print(f"Sending {len(records_to_update)} record status updates to QuickBase...")
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print("✅ Successfully imported data!")
    else:
        print("An error occurred.")
        print(f"Status Code: {response.status_code}")
        print("Response Body:")
        print(response.text)


if __name__ == "__main__":
    datetime_str, msg_str = extract_arbin_log('Arbin_monitor.htm')
    threshold = datetime.now() - timedelta(hours=4)
    activities = extract_latest_activities(datetime_str, msg_str, threshold)
    channel_updates, TN_updates, status_updates = get_updates(activities)
    tester = 'Arbin #8'
    df_qb = fetch_qb_records(tester, channel_updates)
    records_to_import = calculate_status_changes(df_qb, channel_updates, TN_updates, status_updates)
    import_to_qb(records_to_import)
