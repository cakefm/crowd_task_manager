import os
import sys
import shutil
import stat
from pathlib import Path

sys.path.append("..")
import common.settings as settings

def get_sheet_slices_directory(sheet_name:str):
    '''
    Get the path to the slices folder of the current sheet 
    and recursively create the folders if they don't exist.
    '''
    file_path = settings.base_sheet_path / sheet_name / "slices"
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_sheet_pages_directory(sheet_name:str):
    '''
    Get the path to the page images folder of the current sheet 
    and recursively create the folders if they don't exist.
    '''
    file_path = settings.base_sheet_path / sheet_name / "pages"
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_sheet_whole_directory(sheet_name:str):
    '''
    Get the path to the mei/pdf folder of the current sheet 
    and recursively create the folders if they don't exist.
    '''
    file_path = settings.base_sheet_path / sheet_name / "whole"
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_sheet_base_directory(sheet_name:str):
    '''
    Get the path to the root of the given sheet's directory
    and recursively create the folders if they don't exist.
    '''
    file_path = settings.base_sheet_path / sheet_name
    file_path.mkdir(parents=True, exist_ok=True)
    return file_path

def get_sheet_git_directory(sheet_name:str):
    '''
    Get the path to the git folder of the current sheet 
    and recursively create the folders if they don't exist.
    '''
    file_path = settings.base_sheet_path / sheet_name / "git"
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