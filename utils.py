

def repo_path(relative_path):
    import os
    current_dir = os.path.dirname(__file__)
    # relative_path = ".././data/spark_in_action_data/books.csv"
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path