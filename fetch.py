import json, asyncio, time, random, argparse
from src.models import CompetitionModel
from src.fetchers import FETCHER_REGISTRY
from concurrent.futures import ThreadPoolExecutor

MIN_PAUSE = 5
MAX_PAUSE = 10
MAX_PARALLEL = 10

def _run(fetcher, target_dir):
    time.sleep(random.uniform(MIN_PAUSE, MAX_PAUSE))
    fetcher.load_pipeline(target_dir)


async def main(target_dir, *tasks):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(MAX_PARALLEL) as pool:
        await asyncio.gather(*[loop.run_in_executor(pool, _run, c, target_dir) for c in tasks])
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="input.json")
    parser.add_argument("--output", type=str, default="target")
    args = parser.parse_args()
    with open(args.input, "r") as hndl:
        task_dict = json.load(hndl)
    fetch_l = [FETCHER_REGISTRY[f"{d['name']}CompetitionFetcher"](CompetitionModel.from_dict(d)) for d in task_dict]
    asyncio.run(main(args.output, *fetch_l))