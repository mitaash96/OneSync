import os
import time
import polars as pl
import re


def check_tdfile(filepath):
    with open(filepath, 'r', encoding='utf-8') as tdf:
        if '#todoist' in tdf.read():
            return True
        else:
            return False


def extract_outlinks(filepath):
    with open(filepath, 'r', encoding='utf-8') as tdf:
        text = tdf.read()    
    
    pattern = r'\[\[(.*?)\]\]'
    
    matches = re.findall(pattern, text)

    matches = list(map(lambda x: x.split('|')[0], matches))
    
    return matches


def get_fileinfo(filepath):
    _tformat = lambda x: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x))
    
    with open(filepath, 'r', encoding='utf8') as mdfile:
        mdlines = mdfile.readlines()
    
    try:
        _task_ids = [t.split(":")[-1].strip().replace('''"''','') for t in mdlines if t.startswith('taskid')][0].split(',')
    except:
        _task_ids = []

    _tags = []
    
    for line in mdlines:
        if "#" in line and "# " not in line:
            _tags.extend(line.replace("#", '').replace("\n", '').split(' '))
    
    return _tformat(os.path.getctime(filepath)), _tformat(os.path.getmtime(filepath)), _tags, _task_ids


def mine_notes(directory):

    roots = []
    for root, dir, file in os.walk(directory):
        if '.' not in root:
            roots.append(root)

    roots = [(r.rsplit('\\', maxsplit=1)[-1].replace('+', '').strip(), r) for r in roots]

    roots = {
        'folder': [r[0] for r in roots],
        'path': [r[1] for r in roots]
    }

    folders = pl.from_dict(roots)

    folders.write_parquet('src/data/vault_folders')

    keys = {
        root.rsplit("\\", maxsplit=1)[-1]: {
            'path': root, 'files': [f for f in file if f.endswith('.md')]
            }
        for root, dir, file in os.walk(directory)
        if '.' not in root
    }

    keys = {
        k:v for k,v in keys.items()
        if len(v['files'])>0
        }

    all_files = []

    for v in keys.values():
        all_files.extend(list(map(lambda x: f"{v['path']}\\{x}",v['files'])))
        # v['files'] = [f for f in v['files'] if check_tdfile(f"{v['path']}\\{f}")]
        # above demised to find all notes and not just todoist
        # usage in archival feature
        files = []
        for f in v['files']:
            tstuple = get_fileinfo(f"{v['path']}\\{f}")
            files\
                .append(
                    {
                        'title': f,
                        'path': f"{v['path']}\\{f}",
                        'ctime': tstuple[0],
                        'mtime': tstuple[1],
                        'ids': tstuple[3],
                        'tags': tstuple[2]
                    }
                )
        v['files'] = files

    outlinks = [{'file': file, 'outlinks': extract_outlinks(file)} for file in all_files]

    outlinks = pl.from_dicts(outlinks)\
        .explode('outlinks')
    
    outlinks.write_parquet('src/data/outlinks')

    notes = []
    for v in keys.values():
        for f in v['files']:
            notes.append(f)

    table = pl.from_dicts(notes)

    table = table\
        .with_columns(
            pl.col('ctime').str.to_datetime("%Y-%m-%d %H:%M:%S"),
            pl.col('mtime').str.to_datetime("%Y-%m-%d %H:%M:%S")
            )\
        .explode('ids')

    table.write_parquet('src/data/notes')
    print('Vault structure snapshot completed')


if __name__ == '__main__':
    extract_outlinks(r"C:\Users\paulm\OneDrive\OneVault\+ journal\The Plan - 27.md")




