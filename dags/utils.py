import pickle
import os

def _build_files_path(file_name:str):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(root_dir)
    path = os.path.join(root_dir, 'files/' + file_name)
    
    return path

def _save_to_file(file_name:str, data):
    
    path = _build_files_path(file_name)

    with open(path, 'wb') as file:
        pickle.dump(data, file)

    return path


def _read_file(path:str):

    with open(path, 'rb') as file:
        data = pickle.load(file)
        return data


def _del_file(path:str):
    os.remove(path)