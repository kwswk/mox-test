import json
import kaggle
import os
import subprocess
import zipfile


def init_on_kaggle(username, api_key):
    KAGGLE_CONFIG_DIR = os.path.join(os.path.expandvars('$HOME'), '.kaggle')
    os.makedirs(KAGGLE_CONFIG_DIR, exist_ok = True)
    api_dict = {
        "username": username, 
        "key": api_key
    } 
    with open(f"{KAGGLE_CONFIG_DIR}/kaggle.json", "w", encoding='utf-8') as f:
        json.dump(api_dict, f)
    cmd = f"chmod 600 {KAGGLE_CONFIG_DIR}/kaggle.json"
    output = subprocess.check_output(cmd.split(" "))
    output = output.decode(encoding='UTF-8')
    print(output)


def download_datasets(dataset: str, output_path: str) -> None:

    datasets = kaggle.api.dataset_list(search=dataset)
    kaggle.api.dataset_download_files(vars(datasets[0])['ref'], path=output_path)
    with zipfile.ZipFile(f'{output_path}/{vars(datasets[0])["ref"].split("/")[1]}.zip', 'r') as zip_ref:
        zip_ref.extractall(output_path)
