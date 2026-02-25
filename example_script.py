import asyncio
import os
import sys
from pprint import pprint
from time import perf_counter

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))
from nadag_python.nadag_functions import fetch_from_bounds


async def main():
    bounds = [270756.3025, 6663604.1026, 270882.3954, 6663697.4705]
    start_time = perf_counter()
    nadag_data = await fetch_from_bounds(bounds=bounds)
    end_time = perf_counter()
    print(f"Data fetched in {end_time - start_time:.2f} seconds.")
    print("")
    print(nadag_data)

    pd.set_option("display.max_columns", 10)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_colwidth", 20)
    pd.set_option("display.precision", 1)

    pprint(pd.DataFrame(nadag_data.query_sample(sample_id=nadag_data.test_series_data.iloc[0]["prøveseriedelId"])).T)


if __name__ == "__main__":
    asyncio.run(main())
