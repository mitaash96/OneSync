import polars as pl


def execute(_df, folders):
    
    _df = _df\
        .with_columns(
            pl.col('content').str.split(' ').map_elements(lambda x: [w for w in x if not '@' in w])
            .list.join(' ').str.to_titlecase()
        )
    
    label_map = {
        'mindfulness': 'journal',
        'discipline': 'journal',
        'obsidian': 'fleeter',
    }

    
    _df = _df.explode('labels').with_columns(pl.col('labels').fill_null('todoist'))\
        .filter(~(pl.col('labels')=='wait'))\
        .with_columns(pl.col('labels').map_elements(lambda x: label_map[x] if x in label_map.keys() else x))\
        .join(folders, left_on='labels', right_on='folder', how='left')\
        .with_columns(pl.lit(1).alias('rn'))\
        .with_columns(pl.col('rn').cum_count().over('content'))\
        .filter(pl.col('rn')==1)\
        .drop('rn')
    
    lk = {col: _df.columns.index(col) for col in _df.columns}

    for node in _df.iter_rows():
        note_parts = []
        note_parts.extend(['---', f'taskid: "{node[lk['id']]}"', '---'])
        note_parts.append('#'+' #'.join(list(set(['todoist', node[lk['labels']]]))).strip())
        note_parts.append(f"[View in Todoist]({node[lk['url']]})")
        note_parts.append("# Description / Objective")
        note_parts.append("# Notes")

        note = '\n'.join(note_parts)

        clean_str = lambda x: ' '.join([w for w in x.split(' ') if '@' not in w])

        fpath = f"{node[lk['path_right']]}\\{clean_str(node[lk['content']])}.md"

        for _ in "?":
            fpath = fpath.replace(_,'')

        with open(fpath, "w") as file:
            file.write(note)
        
        print(f"Note created at: {fpath}")


def create_notes():
    todoist = pl.read_parquet('src/data/all_tasks')
    obsidian = pl.read_parquet('src/data/notes')
    folders = pl.read_parquet('src/data/vault_folders')
    archive = pl.read_parquet('src/data/archived_notes')
    deleted = pl.read_parquet('src/data/deleted_notes')

    base = todoist.filter(pl.col('parent_id').is_null()).select('id', 'content', 'created_at', 'labels', 'url')\
        .join(obsidian, left_on='id', right_on='ids', how='outer')

    t2o = base\
        .filter(
            pl.col('title').is_null()
            &(~pl.col('id').is_in(archive['todoist_id']))
            &(~pl.col('id').is_in(deleted['ids']))
            )

    t2o = t2o\
        .sort('content', 'created_at', descending=[False, False])\
        .with_columns(pl.lit(1).alias('rn'))\
        .with_columns(pl.col('rn').cum_count().over('content'))\
        .filter(pl.col('rn')==1)\
        .drop('rn')

    if t2o.shape[0] > 0:
        execute(t2o, folders)
    else:
        print("no new notes")


if __name__ == '__main__':
    df = pl.read_parquet('src/data/all_tasks')
    pass
