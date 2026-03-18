import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, field_validator  # Updated import


# 1. Pydantic V2 Models
class ArbinActivity(BaseModel):
    """Validates the structure of a single log entry activity."""
    time: datetime
    channel: str
    test_name: str = Field(alias="TN")
    status_update: str

    @field_validator('channel')
    @classmethod
    def validate_channel(cls, v: str) -> str:
        """Pydantic V2 style validator for the channel field."""
        if v == "N/A":
            return v
        if not v.isdigit():
            raise ValueError(f"Channel must be numeric or N/A, got {v}")
        return v


class QBUpdateRecord(BaseModel):
    """Ensures updates sent to QuickBase match the required JSON structure."""
    test_id: int
    new_status: str

    def to_qb_format(self) -> dict:
        return {
            "3": {"value": self.test_id},
            "76": {"value": self.new_status}
        }


# 2. Arbin Parser Class
class ArbinLogParser:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def _extract_raw_data(self) -> tuple[list[str], list[str]]:
        #
        with open(self.file_path, "r", encoding="windows-1252") as f:
            soup = BeautifulSoup(f, "html.parser")

        timestamps = [[" ".join(ts.text.split())][0][1:21] for ts in soup.find_all("font", color="#008000")]
        messages = [msg.text.strip() for msg in soup.find_all("font", color="#000000")]
        return timestamps, messages

    def get_latest_activities(self, hours_threshold: int = 4) -> list[ArbinActivity]:
        #
        cutoff = datetime.now() - timedelta(hours=hours_threshold)
        dt_str, ms_str = self._extract_raw_data()

        activities = []
        for ts, msg in zip(dt_str, ms_str):
            event_time = datetime.strptime(ts, "%Y-%m-%d, %H:%M:%S")
            if event_time <= cutoff:
                continue

            status = None
            if any(kw in msg for kw in ["Succeeded to start test", "Succeeded to resume"]):
                status = "start"
            elif "Succeeded to stop" in msg:
                status = "stop"

            if status:
                tn_match = re.search(r"start test ([\d_]+)", msg)
                ch_match = re.search(r"Channel (\d+)", msg)

                # Using model_validate (V2) or standard constructor
                activities.append(ArbinActivity(
                    time=event_time,
                    channel=ch_match.group(1) if ch_match else "N/A",
                    TN=tn_match.group(1) if tn_match else "N/A",
                    status_update=status
                ))
        return activities


# 3. QuickBase Client Class
class QuickBaseClient:
    def __init__(self, token: str, table_id: str):
        self.headers = {
            "QB-Realm-Hostname": "https://company.quickbase.com",
            "Authorization": f"QB-USER-TOKEN {token}",
            "Content-Type": "application/json"
        }
        self.table_id = table_id
        self.base_url = "https://api.quickbase.com/v1"

    def fetch_records(self, tester_name: str, channels: list[str]) -> pd.DataFrame:
        #
        full_ids = [f"{tester_name} _ {ch}" for ch in channels]
        query = " OR ".join([f"{{438.EX.'{ch}'}}" for ch in full_ids])

        body = {"from": self.table_id, "select": [438, 177, 3, 76], "where": query}
        r = requests.post(f"{self.base_url}/records/query", headers=self.headers, json=body)
        return pd.json_normalize(r.json().get('data', []))

    def push_updates(self, updates: list[QBUpdateRecord]):
        #
        if not updates:
            print("No updates needed.")
            return

        payload = {
            "to": self.table_id,
            "data": [u.to_qb_format() for u in updates],
            "mergeFieldId": "3"
        }
        r = requests.post(f"{self.base_url}/records", headers=self.headers, json=payload)
        if r.status_code == 200:
            print(f"✅ Successfully updated {len(updates)} records.")
        else:
            print(f"❌ Error: {r.text}")


# 4. Orchestrator (Sync Manager)
class ArbinSyncManager:
    def __init__(self, tester_name: str, log_path: str):
        self.tester_name = tester_name
        self.parser = ArbinLogParser(log_path)
        self.qb = QuickBaseClient(
            token="qb_user_token",
            table_id="bqg4mcgfv"
        )

    def run(self):
        #
        activities = self.parser.get_latest_activities()
        if not activities:
            print("No recent log activity found.")
            return

        activities.sort(key=lambda x: x.time)
        latest_map = {a.channel: a for a in activities}

        df = self.qb.fetch_records(self.tester_name, list(latest_map.keys()))
        if df.empty:
            print("No matching records found in QuickBase.")
            return

        updates = []
        tn_lookup = {a.test_name: a.status_update for a in activities if a.test_name != "N/A"}

        for _, row in df.iterrows():
            fid_map = {"438": "ch_id", "177": "tn", "3": "id", "76": "status"}
            data = {fid_map.get(col.split('.')[0], col): val for col, val in row.items()}

            ch_match = re.search(r" _ (\d+)", str(data['ch_id']))
            ch_num = ch_match.group(1) if ch_match else ""

            new_status = data['status']
            if data['tn'] in tn_lookup:
                new_status = tn_lookup[data['tn']]
            elif ch_num in latest_map:
                new_status = latest_map[ch_num].status_update

            if new_status != data['status']:
                updates.append(QBUpdateRecord(test_id=int(data['id']), new_status=new_status))

        self.qb.push_updates(updates)


if __name__ == "__main__":
    manager = ArbinSyncManager(tester_name="Arbin #8", log_path="Arbin_monitor.htm")
    manager.run()
