from datetime import datetime, timezone
import sqlite3
import os
from queue import Queue
from pathlib import Path
from typing import Optional
import iso8601
import zipfile


def eq_size(first: int, second: int):
    return first == second


def gt_size(first: int, second: int):
    return first > second


def lt_size(first: int, second: int):
    return first < second


def ge_size(first: int, second: int):
    return first >= second


def le_size(first: int, second: int):
    return first <= second


def eq_datetime(first: datetime, second: datetime):
    return first == second


def gt_datetime(first: datetime, second: datetime):
    return first > second


def lt_datetime(first: datetime, second: datetime):
    return first < second


def ge_datetime(first: datetime, second: datetime):
    return first >= second


def le_datetime(first: datetime, second: datetime):
    return first <= second


class CheckAggregator:
    def __init__(self, request: dict):
        self.__text: str = request.get('text')
        if self.__text is not None:
            self.__text = self.__text.encode()
        self.__file_mask: str = request.get('file_mask')
        if self.__file_mask is None:
            self.__file_mask = "*"
        if (size_dict := request.get('size')) is None:
            self.__size_value: Optional[int] = None
        else:
            self.__size_value: Optional[int] = size_dict.get('value')
            self.__size_operator = dict(eq=eq_size, gt=gt_size, lt=lt_size, ge=ge_size,
                                        le=le_size)[size_dict['operator']]
        if (time_dict := request.get('creation_time')) is None:
            self.__time_value: Optional[datetime] = None
        else:
            self.__time_value: Optional[datetime] = iso8601.parse_date(time_dict['value']).astimezone(timezone.utc)
            self.__time_operator = dict(eq=eq_datetime, gt=gt_datetime, lt=lt_datetime, ge=ge_datetime,
                                        le=le_datetime)[time_dict['operator']]

    def get_file_mask(self):
        return self.__file_mask

    def get_text(self):
        return self.__text

    def check(self, path: Path):
        path_info = os.stat(path)
        if self.__size_value is not None and not self.__size_operator(path_info.st_size, self.__size_value):
            return False
        if self.__time_value is not None and not self.__time_operator(
                datetime.fromtimestamp(path_info.st_ctime).astimezone(timezone.utc), self.__time_value):
            return False
        if self.__text is not None:
            with open(path, "rb") as f:
                if self.__text not in f.read():
                    return False
        return True


def check_files(queue: Queue, config, search_bd_id: int, request: dict):
    db = sqlite3.connect(config['DATABASE'])
    db.row_factory = sqlite3.Row

    checker = CheckAggregator(request)
    check_text = checker.get_text()

    for path in Path(config['DATA_FOLDER']).rglob(checker.get_file_mask()):
        if path.is_file():
            if zipfile.is_zipfile(path):
                if check_text is not None:
                    with zipfile.ZipFile(path) as zip_archive:
                        for file in zip_archive.namelist():
                            if not zip_archive.getinfo(file).is_dir():
                                with zip_archive.open(file, 'r') as file_in_zip:
                                    if check_text in file_in_zip.read():
                                        db.execute(
                                            f'insert into PathToFile(parent_index, path) values ({search_bd_id}, '
                                            f'"{os.path.join(path, file)}")')
                                        db.commit()
            elif checker.check(path):
                db.execute(f'insert into PathToFile(parent_index, path) values ({search_bd_id}, "{path}")')
                db.commit()

    db.execute(f'update SearchRequest set status = True where data_index = {search_bd_id}')
    db.commit()
    db.close()
    queue.get()
    queue.task_done()
