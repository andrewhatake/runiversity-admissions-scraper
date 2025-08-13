## About
Light personal tool to scrap and parse admittee data from russian university admissions portals for later analysis

## Quick start
Project uses `pipenv` for venv management.

Competition lists are coded into json input file and passed to `fetch.py` in following format:
```python
{
    "url": "https://cpk.msu.ru/rating/dep_02#02_02_1_02",  # url of admittee list
    "name": "MSU",
    "faculty": "VMK",
    "speciality": "PMI",
    "type": "budget",  # competition type ["major_quota", "minor_quota", "budget", "contract"]
    "limit": 220,  # amount of people passing
    "exs": ["math", "phys/inf", "ru"] # admission exams sequence in order of priority
}
```
The `fetch.py` script scraps admitee lists of competitions found in json into directory of `.parquet` files, partitioned by competition information & timestamp.

Data is used for basic EDA analysis to solve who passes competition in `jupyter`-notebook.
