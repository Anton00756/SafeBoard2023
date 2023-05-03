from flask import Flask, json, request, g, abort
import sqlite3
import os
from uuid import uuid4
from local_file_system import check_files
from threading import Thread
from queue import Queue
from typing import Final
import sys


ADD_NEW_THREAD_IN_QUEUE: Final[int] = 1

threading_queue = Queue()
api = Flask(__name__)
api.config.from_object(__name__)
api.config.update(dict(
    DATABASE=os.path.join(api.root_path, 'flaskr.db'),
    DATA_FOLDER="data_directory",
    SECRET_KEY='DEV_KEY',
    USERNAME='admin',
    PASSWORD='default'
))
api.config.from_envvar('FLASKR_SETTINGS', silent=True)


@api.route('/searches/<search_id>', methods=['GET'])
def get_search_result(search_id):
    db = get_db()
    if (search_info := db.execute(f'select * from SearchRequest where search_id = "{search_id}"').fetchone()) is None:
        abort(400, 'Несуществующий ID')
    if not search_info[-1]:
        return json.dumps(dict(finished=False))
    if (paths := db.execute(f'select path from PathToFile where parent_index = {search_info[0]}').fetchall()) is None:
        return json.dumps(dict(finished=True, paths=[]))
    return json.dumps(dict(finished=True, paths=list(path['path'] for path in paths)))


@api.route('/search', methods=['POST'])
def add_search():
    search_id = uuid4()
    db = get_db()
    db.execute(f'insert into SearchRequest(search_id) values ("{search_id}")')
    db.commit()
    search_bd_id = db.execute(f'select data_index from SearchRequest where search_id="{search_id}"')\
        .fetchone()['data_index']
    threading_queue.put(ADD_NEW_THREAD_IN_QUEUE)
    Thread(target=check_files, args=(threading_queue, api.config, search_bd_id, json.loads(request.get_json())
                                     if request.is_json else {})).start()
    return json.dumps(dict(search_id=search_id))


@api.route('/init_db', methods=['GET'])
def init_db_by_request():
    init_db()
    return "OK"


def connect_db():
    rv = sqlite3.connect(api.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv


def get_db():
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


@api.teardown_appcontext
def close_db(error):
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()


def init_db():
    db = get_db()
    with api.open_resource('schema.sql', mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        api.config["DATA_FOLDER"] = sys.argv[1]
    api.run(debug=True, threaded=True)
