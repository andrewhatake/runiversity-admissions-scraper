import pandas as pd
from curl_cffi import requests
from io import BytesIO
from pathlib import Path
import datetime as dt


class BaseCompetitionFetcher:
    def __init__(self, url: str):
        self.url = url

    def load_pipeline(self):
        raise NotImplementedError


class HSECompetitionFetcher(BaseCompetitionFetcher):
    def __init__(self, url: str):
        super().__init__(url)

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
                "p9", "p10", "priority", "is_eligible", "is_locked"
            ],
            true_values=["Да"], false_values=["Нет"]
        )
        res = (
            df
            .assign(
                exs=df.loc[:, ["ex_math", "ex_opt", "ex_ru"]].astype(str).agg(";".join, axis=1),
                is_ivan_p=df.loc[:, ["p9", "p10"]].any(axis=1)
            )
            .drop(["ex_math", "ex_opt", "ex_ru", "p9", "p10", "ex_sum"], axis=1)
            .fillna(0)
            .sort_values(
                by=["total", "priority", "is_ivan_p"],
                ascending=[False, True, False]
            )
        )
        return res

    def load_pipeline(self, output_dir: str):
        output_dir = Path(output_dir)
        buf = self._fetch_buffer()
        df = self._process_df(buf)
        output_dir.mkdir(parents=True, exist_ok=True)
        (
            df
            .assign(ts=dt.datetime.now().strftime("%Y%m%d_%H%M%S"), ds=self.url.split('/')[-1].split('.')[0])
            .to_parquet(output_dir / "data", engine="pyarrow", partition_cols=["ds", "ts"])
        )