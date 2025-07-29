import pandas as pd
from curl_cffi import requests
from io import BytesIO
from pathlib import Path
import datetime as dt


class HSECompetitionFetcher:
    def __init__(self, url: str, local_dir: str):
        self.url = url
        self.local_dir = Path(local_dir)

    def _fetch_buffer(self):
        resp = requests.get(
            url=self.url,
            impersonate="chrome",
            headers={
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "referer": "https://ba.hse.ru/finlist"
            },
            timeout=5,
            stream=True
        )
        buf = BytesIO()
        try:
            resp.raise_for_status()
            for chunk in resp.iter_content(8192):
                if chunk:
                    buf.write(chunk)
        finally:
            resp.close()
        buf.seek(0)
        return buf

    def _process_df(self, buf: BytesIO):
        df = pd.read_excel(
            buf, sheet_name=0, skiprows=11, header=None, usecols=[1]+list(range(3, 15)),
            names=[
                "id", "agreed", "ex_opt", "ex_math", "ex_ru", "ex_sum", "add_sum", "total",
                "p9", "p10", "priority", "got_anyway", "got_agreedlist"
            ],
            true_values=["Да"], false_values=["Нет"]
        )
        return df.fillna(0).sort_values(
            by=["total", "priority", "p9", "p10", "ex_sum", "ex_math"],
            ascending=[False, True, False, False, False, False]
        )

    def load_pipeline(self):
        buf = self._fetch_buffer()
        df = self._process_df(buf)
        self.local_dir.mkdir(parents=True, exist_ok=True)
        cur_ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        df.to_csv(f"{self.local_dir}/{self.url.split('/')[-1].split('.')[0]}_{cur_ts}.tsv", index=True, sep="\t")