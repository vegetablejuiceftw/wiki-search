import os

import git

ROOT_DIR = git.Repo('.', search_parent_directories=True).working_tree_dir


def reset_working_directory():
    os.chdir(ROOT_DIR)


def cached_model():
    "all-MiniLM-L6-v2"
