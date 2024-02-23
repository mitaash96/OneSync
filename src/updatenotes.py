import polars as pl
import os


def rename_file(path_to_file, new_base_name):
    
    base_name = os.path.basename(path_to_file)
    ext = os.path.splitext(base_name)[1]
    
    new_path_to_file = os.path.join(os.path.dirname(path_to_file), f"{new_base_name}{ext}")
    
    try:
        os.rename(path_to_file, new_path_to_file)
        print(f"File '{path_to_file}' has been renamed to '{new_path_to_file}'.")
    except FileNotFoundError as e:
        print(f"Cannot rename file '{path_to_file}'. Reason: {e}")


def update_notes():
    todoist = pl.read_parquet('src/data/all_tasks')
    obsidian = pl.read_parquet('src/data/notes')
    
    updates = todoist\
        .filter(pl.col('parent_id').is_null())\
        .select('uid', 'id', 'content', 'created_at')\
        .join(
            todoist.select('parent_id', 'created_at'), left_on='id', right_on='parent_id', how='left'
        )\
        .group_by('uid', 'id', 'content', 'created_at').agg(pl.col('created_at_right').max().alias('latest_child_created'))\
        .with_columns(pl.max_horizontal('created_at', 'latest_child_created').alias('task_modified'))\
        .join(
            obsidian.select('ids', 'title', 'ctime', 'mtime', 'path').rename({'path': 'path_right'}), left_on='id', right_on='ids', how='left'
        )\
        .sort('id', 'task_modified', descending=[False, True])\
        .with_columns(pl.lit(1).alias('rn'))\
        .with_columns(pl.col('rn').cum_count().over('id'))\
        .filter(pl.col('rn')==1)\
        .drop('rn')\
        .with_columns(pl.col('content').map_elements(lambda x: ' '.join([w for w in x.split(' ') if '@' not in w and '!' not in w]).strip()))\
        .filter(
            (pl.col('content').str.to_lowercase()!=pl.col('title').str.replace('.md', '').str.to_lowercase())
            & (pl.col('mtime')<=pl.col('task_modified'))
            )


    for path, content in updates.select('path_right', 'content').iter_rows():
        rename_file(path, content)
    
    print("Notes updated successfully")
