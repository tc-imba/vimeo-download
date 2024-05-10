from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
import opendatasets as od
import asyncio
import functools
from loguru import logger
from tqdm import tqdm
import shutil

logger.remove()
logger.add(lambda msg: tqdm.write(msg, end=""))


def make_sync(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return wrapper


@click.group()
def cli_group():
    pass


@cli_group.command()
@click.option("--owner", default="wangsally")
@click.option("--dataset", default="vimeo-90k-00001")
def download(owner: str, dataset: str):
    dataset_url = f"https://www.kaggle.com/datasets/{owner}/{dataset}"
    dataset_dir = Path("dataset")
    dataset_dir.mkdir(parents=True, exist_ok=True)
    od.download(dataset_url, str(dataset_dir.resolve()))


def parse_sep_file(sep_file: Path):
    result = set()
    with sep_file.open("r") as f:
        for line in f:
            result.add(line.strip())
    return result


@cli_group.command()
@click.option("--dataset", default="vimeo-90k-00001")
@click.option("--workers", default=20)
def parse(dataset, workers):
    dataset_path = Path("dataset") / dataset
    if not (dataset_path / "sequences").exists():
        for path in dataset_path.iterdir():
            if path.is_dir() and (path / "sequences").exists():
                dataset_path = path
                break
    if not (dataset_path / "sequences").exists():
        logger.error("dataset error!")
        exit(-1)

    test_files = parse_sep_file(dataset_path / "sep_testlist.txt")
    train_files = parse_sep_file(dataset_path / "sep_trainlist.txt")
    data_files = dataset_path / "sequences"

    major_indexes = []
    for major_index_dir in data_files.iterdir():
        if major_index_dir.is_dir():
            major_indexes.append(major_index_dir.name)

    def parse_minor_dir(major_index, minor_index_dir):
        minor_index = minor_index_dir.name
        if f"{major_index}/{minor_index}" in test_files:
            # logger.info("{}/{} in test!", major_index, minor_index)
            file_type = "test"
        elif f"{major_index}/{minor_index}" in train_files:
            # logger.info("{}/{} in train!", major_index, minor_index)
            file_type = "train"
        else:
            # logger.warning("{}/{} not in train or test!", major_index, minor_index)
            file_type = "other"

        shutil.copytree(minor_index_dir, dataset_path / file_type / f"{major_index}-{minor_index}",
                        dirs_exist_ok=True)

    with tqdm(total=len(major_indexes) * 1000) as pbar:
        for major_index in major_indexes:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(parse_minor_dir, major_index, minor_index_dir) for minor_index_dir in
                           (data_files / str(major_index)).iterdir()]
                for future in as_completed(futures):
                    result = future.result()
                    pbar.update(1)

    pbar.close()


if __name__ == '__main__':
    cli_group()
