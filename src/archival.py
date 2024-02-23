import polars as pl
import zipfile
import os
from datetime import datetime


def create_zip_archive(file_paths, zip_file_name):
    if os.path.exists(zip_file_name):
        os.remove(zip_file_name)
    
    with zipfile.ZipFile(zip_file_name, 'w') as zipf:
        for file_path in file_paths:
            zipf.write(file_path, os.path.basename(file_path))
            os.remove(file_path)


def archival_process(vault_path):
    df = pl.read_parquet('src/data/outlinks')
    notes = pl.read_parquet('src/data/notes')

    df = df\
        .unique()\
        .with_columns(pl.col('file').str.split('\\').list.get(-1).str.replace('.md', '').alias('fname'))
    
    df = df\
        .with_columns(pl.col('outlinks').is_not_null().cast(pl.Int32))\
        .group_by('file', 'fname').agg(pl.col('outlinks').sum().alias('outlink_cnt'))\
        .join(
            df.group_by('outlinks').agg(pl.col('outlinks').count().alias('inlink_cnt')),
            left_on='fname', right_on='outlinks', how='outer'
        )\
        .filter(pl.col('file').is_not_null())\
        .drop('outlinks')\
        .with_columns(pl.col('inlink_cnt').fill_null(0))
    
    orphans = df.filter((pl.col('outlink_cnt')+pl.col('inlink_cnt'))==0)

    orphans = orphans\
        .join(notes, left_on='file', right_on='path')\
        .with_columns(pl.lit(datetime.now()).alias('curr_time'))\
        .with_columns((pl.col('curr_time')-pl.max_horizontal('ctime', 'mtime')).dt.total_days().alias('idle_time'))\
        .sort('idle_time')\
        .select('file', 'fname', 'ctime', 'mtime', 'ids', 'idle_time')\
        .rename({'ids': 'todoist_id'})
    
    archive_notes = orphans.filter(pl.col('idle_time')>60)
    print(f"Count of notes being sent to archival: {archive_notes.shape[0]}")

    if archive_notes.shape[0]>0:
        archival_log = '\n'.join(list(map(lambda x: f"[[{x}]]",archive_notes['fname'].to_list())))
        
        create_zip_archive(archive_notes['file'].to_list(), f"{vault_path}\\archive.zip")
        
        with open(f"{vault_path}\\Archived Notes.md", 'w') as arch_file:
            arch_file.write(archival_log)
        
        archive_notes.write_parquet('src/data/archived_notes')