import polars as pl
import zipfile
import os
from datetime import datetime
from .ziputils import add_files_to_zip, extract_and_remove_from_zip


def archival_process(_outlinks, _notes):

    _outlinks = _outlinks\
        .unique()\
        .with_columns(pl.col('file').str.split('\\').list.get(-1).str.replace('.md', '').alias('fname'))
    
    _outlinks = _outlinks\
        .with_columns(pl.col('outlinks').is_not_null().cast(pl.Int32))\
        .group_by('file', 'fname').agg(pl.col('outlinks').sum().alias('outlink_cnt'))\
        .join(
            _outlinks.group_by('outlinks').agg(pl.col('outlinks').count().alias('inlink_cnt')),
            left_on='fname', right_on='outlinks', how='outer'
        )\
        .filter(pl.col('file').is_not_null())\
        .drop('outlinks')\
        .with_columns(pl.col('inlink_cnt').fill_null(0))
    
    orphans = _outlinks.filter((pl.col('outlink_cnt')+pl.col('inlink_cnt'))==0)

    orphans = orphans\
        .join(_notes, left_on='file', right_on='path')\
        .with_columns(pl.lit(datetime.now()).alias('curr_time'))\
        .with_columns((pl.col('curr_time')-pl.max_horizontal('ctime', 'mtime')).dt.total_days().alias('idle_time'))\
        .sort('idle_time')\
        .select('file', 'fname', 'ctime', 'mtime', 'ids', 'idle_time')\
        .rename({'ids': 'todoist_id'})
    
    archive_notes = orphans\
        .filter(pl.col('idle_time')>60)\
        .with_columns(pl.lit(datetime.now()).alias('archival_timestamp'))

    return archive_notes
 

def retrieval_process(_outlinks, _archived_notes):
    
    _outlinks = _outlinks\
        .unique()\
        .with_columns(pl.col('file').str.split('\\').list.get(-1).str.replace('.md', '').alias('fname'))\
        .filter(~(pl.col('fname').is_in(['Archived Notes'])))
    
    restore = _archived_notes.join(_outlinks, left_on='fname', right_on='outlinks', how='inner')\
        .select('file').rename({'file': 'restore_fpath'})
    
    if restore.shape[0]>0:
        restore = restore\
            .with_columns(pl.col('restore_fpath').map_elements(lambda x: x.rsplit("\\", maxsplit=1)))\
            .with_columns(
                pl.col('restore_fpath').list.get(-1).alias('file'),
                pl.col('restore_fpath').list.get(0).alias('dir_path')
                )\
            .drop('restore_fpath')
        
    return restore


def archival_log_snapshot(arch_new, arch_old, archive_path=None):

    arch = arch_old\
        .vstack(arch_new.select(arch_old.columns))\
        .sort(by=['fname', 'archival_timestamp'], descending=[False, True])\
        .with_columns(pl.lit(1).alias('rn'))\
        .with_columns(pl.col('rn').cum_count().over('fname'))\
        .filter(pl.col('rn')==1)\
        .drop('rn')
    
    if os.path.exists(archive_path):
        with zipfile.ZipFile(archive_path, 'r') as zipf:
            arch_phys = zipf.namelist()
            arch = arch.filter(pl.col('fname').is_in(list(map(lambda x: x.replace('.md', ''), arch_phys))))
    else:
        for col in arch:
            arch = arch.with_columns(pl.lit(None).cast(arch[col].dtype))
        arch = arch.filter(pl.col('fname').is_not_null())
    
    return arch


def sync_archive(vault_path):
    outlinks = pl.read_parquet('src/data/outlinks')
    notes = pl.read_parquet('src/data/notes')
    archived_notes = pl.read_parquet('src/data/archived_notes')

    archive_path = f"{vault_path}\\archive.zip"

    if os.path.exists(archive_path):
        restore_notes = retrieval_process(outlinks, archived_notes)
        if restore_notes.shape[0]>0:
            print(f"Number of notes being restored: {restore_notes.shape[0]}")
            extract_and_remove_from_zip(
                archive_path, dict(
                    zip(restore_notes['file'].to_list(), restore_notes['dir_path'].to_list())
                    )
                )

    to_archive = archival_process(outlinks, notes)

    if to_archive.shape[0]>0:
        add_files_to_zip(archive_path, to_archive['file'].to_list())
    
    archived_notes = archival_log_snapshot(arch_new=to_archive, arch_old=archived_notes, archive_path=archive_path)

    print(f"{archived_notes.shape[0]} notes in archive")
    archived_notes.write_parquet('src/data/archived_notes')


if __name__ == '__main__':
    df = pl.read_parquet('src/data/archived_notes')
    print(df.head())
    pass