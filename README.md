# VIMEO Dataset Download

## Preparation

Create API Token in https://www.kaggle.com/settings and put `kaggle.json` in this directory.

## Download

Command
```bash
python3 main.py download --author <author> --dateset <dataset>
```

Example
```bash
python3 main.py download --dataset vimeo-90k-00001
python3 main.py download --dataset vimeo-90k-1
```

## Parse

Command
```bash
python3 main.py parse --dateset <dataset>
```

Example
```bash
python3 main.py parse --dataset vimeo-90k-00001
python3 main.py parse --dataset vimeo-90k-1
```
