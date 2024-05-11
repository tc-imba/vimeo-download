from loguru import logger

from main import cli_group


def main():
    datasets = [f"vimeo-90k-{i}" for i in range(1, 11)]
    for dataset in datasets:
        logger.info("download {}", dataset)
        cli_group(["download", "--dataset", dataset], standalone_mode=False)
        logger.info("parse {}", dataset)
        cli_group(["parse", "--dataset", dataset], standalone_mode=False)


if __name__ == '__main__':
    main()
