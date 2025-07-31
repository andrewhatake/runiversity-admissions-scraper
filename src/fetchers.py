import pandas as pd
import numpy as np
import datetime as dt

from curl_cffi import requests
from io import BytesIO, StringIO
from pathlib import Path
from bs4 import BeautifulSoup, SoupStrainer


class BaseCompetitionFetcher:
    def __init__(self, url: str):
        self.url = url

    def load_pipeline(self, output_dir: str):
        output_dir = Path(output_dir)
        buf = self._fetch_buffer()
        df = self._process_df(buf)
        output_dir.mkdir(parents=True, exist_ok=True)
        (
            df
            .assign(ts=dt.datetime.now().strftime("%Y%m%d_%H"), ds=self.url.split('/')[-1].split('.')[0])
            .to_parquet(output_dir / "data", engine="pyarrow", partition_cols=["ts", "ds"], index=False)
        )


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


class MSUCompetitionFetcher(BaseCompetitionFetcher):
    def __init__(self, url: str):
        url_, tag_id_ = url.split("#")
        super().__init__(f"{url_}-{tag_id_}")

    def _fetch_buffer(self):
        res = None
        url_, tag_id_ = self.url.rsplit("-", 1)
        resp = requests.get(
            url=url_,
            impersonate="chrome",
            headers={
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "referer": "https://cpk.msu.ru/"
            },
            timeout=5
        )
        try:
            resp.raise_for_status()
            strainer = SoupStrainer(["h4", "table"])
            soup = BeautifulSoup(resp.text, "lxml", parse_only=strainer)
            res = StringIO(str(soup.find("h4", id=tag_id_).find_next("table")))
        finally:
            resp.close()
        return res

    def _process_df(self, buf):
        df = pd.read_html(buf)[0].replace({"Да": True, "Нет": False}).fillna(0)
        tmp = df.iloc[:, np.r_[1:6, 7:9]]
        tmp.columns = ["id", "agreed", "priority", "is_eligible", "is_locked", "total", "add_sum"]
        return (
            tmp
            .assign(exs=df.iloc[:, 9:-3].astype(str).agg(";".join, axis=1), is_ivan_p=df.iloc[:, -3])
            .sort_values(
                by=["total", "priority", "is_ivan_p"],
                ascending=[False, True, False]
            )
        )


class MIPTCompetitionFetcher(BaseCompetitionFetcher):
    def __init__(self, url: str):
        super().__init__(url)

    def _fetch_buffer(self):
        resp = requests.get(
            url=self.url,
            impersonate="chrome",
            headers={
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "referer": "https://priem.mipt.ru"
            }
        )
        try:
            resp.raise_for_status()
            strainer = SoupStrainer(["table"])
            soup = BeautifulSoup(resp.text, "lxml", parse_only=strainer)
            res = StringIO(str(soup.find("table")))
        finally:
            resp.close()
        return res

    def _process_df(self, buf):
        df = pd.read_html(buf)[0]
        tmp = df.iloc[:, np.r_[1:6, 7]]
        tmp.columns = ["priority", "id", "is_eligible", "is_locked", "total", "add_sum"]
        tmp = tmp.assign(
            agreed=~df.iloc[:, 11].isna(),
            is_eligible=tmp.loc[:, "is_eligible"].astype(bool),
            is_locked=tmp.loc[:, "is_locked"].astype(bool),
            is_ivan_p=~df.iloc[:, 14:17].isna().any(axis=1),
            exs=df.iloc[:, 18:-4:3].fillna("0").replace({"Неявка": "0"}).astype(str).agg(";".join, axis=1)
        )
        tmp.loc[:, "exs"] = tmp.loc[:, "exs"].mask(((tmp.total < 50) & (tmp.is_ivan_p)), "100;100;100;100;100")
        tmp.loc[:, "total"] = tmp.loc[:, "total"].mask(((tmp.total < 50) & (tmp.is_ivan_p)), 500)
        return tmp.sort_values(
            by=["total", "priority", "is_ivan_p"],
            ascending=[False, True, False]
        )