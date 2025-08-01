import json, asyncio, time, random, argparse, logging
from src.models import CompetitionModel
from src.fetchers import FETCHER_REGISTRY
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s::%(levelname)s::%(message)s',
    filename='fetch.log',
    filemode='w'
)


MIN_PAUSE = 10
MAX_PAUSE = 20
MAX_PARALLEL = 20


def _run(fetcher, target_dir):
    time.sleep(random.uniform(MIN_PAUSE, MAX_PAUSE))
    logging.info(f"fetching URL: {fetcher.url}")
    try:
        fetcher.load_pipeline(target_dir)
        logging.info(f"successfully fetched URL: {fetcher.url}")
    except Exception as e:
        logging.error(f"Failed to fetch URL: {fetcher.url} with error: {e}", exc_info=True)
        # Optionally, re-raise or handle as needed for asyncio/ThreadPoolExecutor


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
    random.shuffle(fetch_l)
    asyncio.run(main(args.output, *fetch_l))