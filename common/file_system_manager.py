import os
import sys
import shutil
import stat
from pathlib import Path

sys.path.append("..")
from common.settings import cfg

def get_sheet_slices_directory(sheet_name:str):
    '''
    Get the path to the slices folder of the current sheet 
    and recursively create the folders if they don't exist.
    '''
    file_path = cfg.base_sheet_path / sheet_name / "slices"
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_sheet_pages_directory(sheet_name:str):
    '''
    Get the path to the page images folder of the current sheet 
    and recursively create the folders if they don't exist.
    '''
    file_path = cfg.base_sheet_path / sheet_name / "pages"
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_sheet_whole_directory(sheet_name:str):
    '''
    Get the path to the mei/pdf folder of the current sheet 
    and recursively create the folders if they don't exist.
    '''
    file_path = cfg.base_sheet_path / sheet_name / "whole"
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_sheet_base_directory(sheet_name:str):
    '''
    Get the path to the root of the given sheet's directory
    and recursively create the folders if they don't exist.
    '''
    file_path = cfg.base_sheet_path / sheet_name
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_sheet_git_directory(sheet_name:str):
    '''
    Get the path to the git folder of the current sheet 
    and recursively create the folders if they don't exist.
    '''
    file_path = cfg.base_sheet_path / sheet_name / "git"
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_sheet_api_directory(sheet_name:str, nesting:int=1, slice_type:str=None):
    '''
    Get the path to the static api folder of the current sheet 
    and recursively create the folders if they don't exist.

    Optionally provide the nesting level for the module this is
    being called from, by default it is 1.

    When given a slice type, it will extend the path to the folder
    of that type of slice and make sure it exists.
    '''
    root = Path.cwd()
    for _ in range(nesting):
        root = root.parent

    file_path = root / "api" / "static" / sheet_name
    if slice_type:
        file_path = file_path / "slices" / slice_type
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_clean_sheet_git_directory(sheet_name:str):
    '''
    Empties the sheet's git folder by remaking it and returning the new path.
    '''
    # Needed in case of trouble with the .git folder, mainly problematic on Windows
    def on_rm_error(func, path, exc_info):
        os.chmod(path, stat.S_IWRITE)
        os.unlink(path)

    git_dir = get_sheet_git_directory(sheet_name)
    shutil.rmtree(str(git_dir), onerror=on_rm_error)
    return get_sheet_git_directory(sheet_name)