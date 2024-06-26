from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
import opendatasets as od
import asyncio
import functools
from loguru import logger
from tqdm import tqdm
import os
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
    if (dataset_dir / dataset).exists():
        logger.warning("dataset {} already exists!", dataset)
        return
    od.download(dataset_url, str(dataset_dir.absolute()))


def parse_sep_file(sep_file: Path):
    result = set()
    with sep_file.open("r") as f:
        for line in f:
            result.add(line.strip())
    return result


def parse_dataset_path(dataset_path: Path):
    for file in dataset_path.rglob("*.png"):
        logger.info(f"{file}")

        if len(file.parent.name) == 4:
            return file.parent.parent.parent
    return None


def link_dir(src_path: Path, dest_path: Path):
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.unlink(missing_ok=True)
        if os.name == 'nt':
            os.symlink(str(src_path.absolute()), str(dest_path.absolute()))
        else:
            os.system(f"ln -s {src_path.absolute()} {dest_path.absolute()}")
    except Exception as e:
        logger.exception(e)

@cli_group.command()
@click.option("--dataset", default="vimeo-90k-00001")
@click.option("--workers", default=20)
def parse(dataset, workers):
    dataset_path = Path("dataset") / dataset
    dataset_path = parse_dataset_path(dataset_path)
    if dataset_path is None:
        logger.error("dataset error!")
        return

    test_files = parse_sep_file(dataset_path.parent / "sep_testlist.txt")
    train_files = parse_sep_file(dataset_path.parent / "sep_trainlist.txt")

    major_indexes = []
    for major_index_dir in dataset_path.iterdir():
        if major_index_dir.is_dir():
            major_indexes.append(major_index_dir.name)

    def parse_minor_dir(major_index, minor_index_dir: Path):
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

        dest_path = dataset_path.parent / file_type / f"{major_index}-{minor_index}"
        link_dir(minor_index_dir, dest_path)
        # shutil.copytree(minor_index_dir, dataset_path / file_type / f"{major_index}-{minor_index}",
        #                 dirs_exist_ok=True)

    with tqdm(total=len(major_indexes) * 1000) as pbar:
        for major_index in major_indexes:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(parse_minor_dir, major_index, minor_index_dir) for minor_index_dir in
                           (dataset_path / str(major_index)).iterdir()]
                for future in as_completed(futures):
                    result = future.result()
                    pbar.update(1)

    pbar.close()


@cli_group.command()
@click.option("--input", default="vimeo-90k-*")
@click.option("--output", default="merged-vimeo-90k")
@click.option("--workers", default=20)
def merge(input, output, workers):
    dataset_path = Path("dataset")
    output_dataset_path = dataset_path / output
    output_dataset_path.mkdir(parents=True, exist_ok=True)
    sub_dataset_paths = []

    for sub_dataset_path in dataset_path.glob(input):
        sub_dataset_path = parse_dataset_path(sub_dataset_path)
        if sub_dataset_path is not None:
            sub_dataset_paths.append(sub_dataset_path)

    def worker(src_path, file_type):
        if src_path.is_dir():
            dest_path = output_dataset_path / file_type / src_path.name
            link_dir(src_path, dest_path)


    with tqdm(total=len(sub_dataset_paths) * 10000) as pbar:
        for sub_dataset_path in sub_dataset_paths:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = []
                for file_type in ("test", "train", "other"):
                    for src_path in (sub_dataset_path.parent / file_type).iterdir():
                        futures.append(ex.submit(worker, src_path, file_type))
                pbar.total += len(futures) - 10000
                for future in as_completed(futures):
                    result = future.result()
                    pbar.update(1)


if __name__ == '__main__':
    cli_group()
