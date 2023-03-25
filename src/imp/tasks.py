from datetime import datetime, timedelta
from glob import glob

import jpholiday
import pandas as pd
from invoke import Collection, Context
from src.utils import add_create_delete_task, task

import_tasks = Collection("import")
sql_paths = glob("src/imp/sql/*.sql")
add_create_delete_task(import_tasks, sql_paths, dataset_id="import")


class NewYearHoliday(jpholiday.OriginalHoliday):
    def _is_holiday(self, date):
        # 12/29-01/03 は年末年始休暇とする
        if (date.month == 1 and date.day < 4) or (date.month == 12 and date.day > 28):
            return True
        return False

    def _is_holiday_name(self, date):
        return "年末年始休暇"


@task
def holiday_master(c: Context, start_date: str = "2016-01-01", years: int = 10):

    HOLIDAY_TABLE_SCHEMA = [
        {
            "name": "jst_date",
            "type": "DATE",
            "mode": "REQUIRED",
            "description": "JST基準の日付",
        },
        {
            "name": "is_holiday",
            "type": "BOOL",
            "mode": "REQUIRED",
            "description": "休日(土日 or 祝日)か否か",
        },
        {
            "name": "dayofweek",
            "type": "INT64",
            "mode": "REQUIRED",
            "description": "BQベースのdow",
        },
        {
            "name": "day_type",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "平日 or 土日 or 祝日",
        },
        {
            "name": "day_type_sequence",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "翌日と前日を考慮した区別",
        },
    ]

    base_date = datetime.fromisoformat(start_date)
    end_date = base_date + timedelta(days=365 * years)
    holiday_master_df = pd.DataFrame(
        # 前後の日の情報を使うので1日バッファを持って多く生成
        {
            "jst_date": pd.date_range(
                base_date - timedelta(days=1), end_date + timedelta(days=1)
            )
        }
    )
    holiday_master_df["dayofweek"] = (
        holiday_master_df["jst_date"].dt.dayofweek + 1
    ) % 7 + 1
    holiday_master_df["day_type"] = holiday_master_df.apply(
        # 優先順位: 土日、祝日、平日
        lambda row: "土日"
        if row.dayofweek in {5, 6}
        else jpholiday.is_holiday_name(row.jst_date),
        axis=1,
    )
    holiday_master_df.day_type.fillna("平日", inplace=True)
    holiday_master_df["is_holiday"] = (holiday_master_df.day_type != "平日").astype(int)
    holiday_master_df["is_next_holiday"] = holiday_master_df.is_holiday.shift(-1)
    holiday_master_df["is_prev_holiday"] = holiday_master_df.is_holiday.shift(1)
    # shift用に作成した日付を削除
    holiday_master_df.dropna(inplace=True)
    holiday_master_df["day_type_sequence"] = holiday_master_df[
        ["is_prev_holiday", "is_holiday", "is_next_holiday"]
    ].apply(lambda row: "".join([str(int(x)) for x in row]), axis=1)
    holiday_master_df["jst_date"] = holiday_master_df["jst_date"].dt.date
    holiday_master_df[
        ["jst_date", "is_holiday", "dayofweek", "day_type", "day_type_sequence"]
    ].to_gbq(
        project_id=c.env.gcp_project,
        destination_table="import.holiday_master",
        if_exists="replace",
        table_schema=HOLIDAY_TABLE_SCHEMA,
    )


import_tasks.add_task(holiday_master, "holiday_master")
