import polars as pl
import requests as re
from todoist_api_python.api import TodoistAPI
from datetime import datetime


def fetch_complete(since, user_tk, api, fetch_limit=200):
    url = 'https://api.todoist.com/sync/v9/completed/get_all'
    headers = {"Authorization": f"Bearer {user_tk}"}
    params = {"limit": fetch_limit, "since": since}
    load_time = datetime.now()
    time_format_std = "%Y-%m-%d %H:%M:%S"

    items = re.get(url=url, headers=headers, params=params).json()['items']

    if len(items) == 0:
        return None

    _df = pl.from_dicts(items)\
        .select("id", "task_id", "completed_at")\
        .rename({"id": "uid"})\
        .with_columns(pl.col('task_id').map_elements(lambda x: api.get_task(x).to_dict()).alias('payload'))
    
    payload_cols = _df['payload'].struct.fields

    _df = _df.select("uid", "completed_at", *[pl.col('payload').struct.field(col) for col in payload_cols])

    if _df['due'].dtype == pl.Null:
        due_cols = {'date': pl.Utf8, 'is_recurring': pl.Boolean, 'string': pl.Utf8, 'datetime': pl.Utf8}
        _df = _df.with_columns(*[pl.lit(None).cast(dtype).alias(f"due_{col}") for col, dtype in due_cols.items() if col != 'timezone'])\
            .drop('due')
                
    else:
        due_cols = _df['due'].struct.fields

        _df = _df\
            .hstack(
                _df.select(
                    [pl.col('due').struct.field(col).alias(f"due_{col}") for col in due_cols if col != 'timezone']
                        )
                )\
            .drop('due')
    
    if _df['labels'].dtype == pl.List(pl.Null):
        _df = _df.with_columns(pl.col('labels').cast(pl.List(pl.Utf8)))
    
    for _ in _df.columns:
        if 'datetime' in _:
            _df = _df.with_columns(pl.col(_).cast(pl.Utf8))
    
    _df =  _df.with_columns(
            (
                pl.col("completed_at").str.to_datetime().dt.strftime(time_format_std).str.to_datetime()
                + pl.duration(hours=5, minutes=30)
            ),
            pl.col("due_datetime").str.to_datetime().dt.strftime(time_format_std).str.to_datetime(),
            (
                pl.col("created_at").str.to_datetime().dt.strftime(time_format_std).str.to_datetime()
                + pl.duration(hours=5, minutes=30)
            ),
            pl.lit(load_time).alias('load_timestamp')
        )
        
    return _df


def fetch_active(completed_tasks, api):
    time_format_std = "%Y-%m-%d %H:%M:%S"

    _df = pl.from_dicts([task.to_dict() for task in api.get_tasks()])
    
    due_cols = _df['due'].struct.fields
    
    _df = _df\
        .hstack(
                _df.select(
                    [pl.col('due').struct.field(col).alias(f"due_{col}") for col in due_cols if col != 'timezone']
                        )
                )\
        .drop('due')
    
    _df = _df\
        .with_columns(
            pl.lit(datetime.now()).alias('load_timestamp')
        )
    
    
    for col, dtype in [(col, completed_tasks[col].dtype) for col in completed_tasks.columns if col not in _df.columns]:
        _df = _df.with_columns(pl.lit(None).cast(dtype).alias(col))

    _df = _df\
        .with_columns(
            pl.col("due_datetime").str.to_datetime().dt.strftime(time_format_std).str.to_datetime(),
            pl.col("created_at").str.to_datetime().dt.strftime(time_format_std).str.to_datetime()
        )

    return _df

def task_miner(token):
    
    user_tk = token
    api = TodoistAPI(user_tk)
    time_format_fetch = "%Y-%m-%dT%H:%M:%S"
    
    hist = pl.read_parquet('src/data/completed_tasks_history')
    ts_max_complete = hist['completed_at'].max().strftime(time_format_fetch)
    # ts_max_complete = "2023-01-01T00:00:00"

    print(f"Existing max completion for tasks: {ts_max_complete}")

    completed_tasks = fetch_complete(ts_max_complete, user_tk, api)


    if completed_tasks is not None:
        completed_tasks = completed_tasks.vstack(hist).sort('completed_at', descending=True)
    else:
        completed_tasks = hist.sort('completed_at', descending=True)


    completed_tasks.write_parquet('src/data/completed_tasks_history')

    active_tasks = fetch_active(completed_tasks, api)

    all_tasks = active_tasks.select(completed_tasks.columns).vstack(completed_tasks)

    sep = all_tasks\
        .filter(
            (pl.col('content').str.to_lowercase().is_duplicated())
            &(~(pl.concat_str('id', pl.col('content').str.to_lowercase()).is_duplicated()))
            &(pl.col('parent_id').is_null())
            )


    all_tasks = all_tasks.filter(~pl.col('content').str.to_lowercase().is_in(sep['content'].str.to_lowercase()))

    all_tasks = all_tasks\
        .vstack(
            sep\
                .with_columns(pl.lit(1).alias('rn'), pl.col('content').str.to_lowercase().alias('key'))\
                .sort('key', 'created_at', descending=[False, True])\
                .with_columns(pl.col('rn').cum_count().over('key'))\
                .filter(pl.col('rn')==1)\
                .drop(['rn', 'key'])
            )

    # for col in [col for col in all_tasks.columns if all_tasks[col].dtype in list(pl.DATETIME_DTYPES) and 'at' in col]:
    #     all_tasks = all_tasks.with_columns(pl.col(col) + pl.duration(hours=5, minutes=30))

    all_tasks.write_parquet('src/data/all_tasks')
    print(f"Total tasks written: {all_tasks.shape[0]}")


if __name__ == '__main__':
    pl.read_parquet('src/data/completed_tasks_history')

