import pandas as pd
import numpy as np
import datetime as dt

from curl_cffi import requests
from io import BytesIO, StringIO
from pathlib import Path
from bs4 import BeautifulSoup, SoupStrainer
from .models import CompetitionModel, COMP_COLS

import warnings
warnings.simplefilter('ignore', FutureWarning)


class BaseCompetitionFetcher:
    def __init__(self, comp: CompetitionModel):
        self.comp = comp
    
    @property
    def url(self):
        return self.comp.url

    def load_pipeline(self, output_dir: str):
        output_dir = Path(output_dir)
        buf = self._fetch_buffer()
        df = self._process_df(buf).sort_values(
            by=["total", "priority", "is_ivan_p"],
            ascending=[False, True, False]
        )
        part_dict = {k: getattr(self.comp, k) for k in COMP_COLS}
        output_dir.mkdir(parents=True, exist_ok=True)
        (
            df
            .assign(ts=dt.datetime.now().strftime("%Y%m%d_%H"), **part_dict)
            .drop_duplicates(subset=["id"])
            .to_parquet(output_dir / "data", engine="pyarrow", partition_cols=list(part_dict.keys())+["ts"], index=False)
        )


class HSECompetitionFetcher(BaseCompetitionFetcher):
    def _fetch_buffer(self):
        resp = requests.get(
            url=self.url,
            impersonate="chrome",
            headers={
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "referer": "https://ba.hse.ru/finlist"
            },
            timeout=15,
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
                "id", "agreed", "ex_1", "ex_2", "ex_3", "ex_sum", "add_sum", "total",
                "p9", "p10", "priority", "is_eligible", "is_locked"
            ],
            true_values=["Да"], false_values=["Нет"]
        )
        res = (
            df
            .assign(
                exs=df.loc[:, ["ex_1", "ex_2", "ex_3"]].astype(str).agg(";".join, axis=1),
                is_ivan_p=df.loc[:, ["p9", "p10"]].any(axis=1)
            )
            .drop(["ex_1", "ex_2", "ex_3", "p9", "p10", "ex_sum"], axis=1)
            .fillna(0)
        )
        return res


class MSUCompetitionFetcher(BaseCompetitionFetcher):
    def _fetch_buffer(self):
        res = None
        url_, tag_id_ = self.url.rsplit("#", 1)
        resp = requests.get(
            url=url_,
            impersonate="chrome",
            headers={
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "referer": "https://cpk.msu.ru/"
            },
            timeout=15
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
        df = pd.read_html(buf)[0].fillna(0)
        tmp = df.loc[df.iloc[:, -1] == "В конкурсе"].iloc[:, np.r_[1:6, 7:9]]
        tmp.columns = ["id", "agreed", "priority", "is_eligible", "is_locked", "total", "add_sum"]
        return (
            tmp
            .assign(exs=df.iloc[:, 9:-3].astype(str).agg(";".join, axis=1), is_ivan_p=df.iloc[:, -3].str[:3].str.strip())
            .replace({"Да": True, "Нет": False})
            .dropna(axis=0)
            
        )

    def load_ivan_p(self, output_dir: str):
        output_dir = Path(output_dir)
        buf = self._fetch_buffer()
        df = pd.read_html(buf)[0].fillna(0)
        with open(output_dir / "msu.txt", "a") as hndl:
            for p in df.iloc[:, 1].astype(str).str[-6:].str.lstrip('0'):
                hndl.write(f"{p}\n")



class MIPTCompetitionFetcher(BaseCompetitionFetcher):
    def _fetch_buffer(self):
        resp = requests.get(
            url=self.url,
            impersonate="chrome",
            headers={
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "referer": "https://priem.mipt.ru"
            },
            timeout=15
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
        tmp = df.loc[df.iloc[:, 9] == "Да"].iloc[:, np.r_[1:6, 7]]
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
        return tmp.dropna(axis=0)


FETCHER_REGISTRY = {
    cls.__name__: cls
    for cls in BaseCompetitionFetcher.__subclasses__()
}