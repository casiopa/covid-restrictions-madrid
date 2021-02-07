import json
import sqlite3

conn = sqlite3.connect('areas.sqlite')
cur = conn.cursor()


### Create and fill table for health areas (zbs) related with its fathers
fname = 'mun_distr_childs.json'
str_mun_distr_childs = open(fname).read()
json_mun_distr_childs = json.loads(str_mun_distr_childs)

cur.execute('DROP TABLE IF EXISTS ZBS_fathers')
cur.execute('''CREATE TABLE ZBS_fathers (zbs_id INTEGER UNIQUE, mun_distr_id INTEGER)''')

for mun_distr in json_mun_distr_childs :
    cur.execute('SELECT id FROM MunDistr WHERE name = ? LIMIT 1', (mun_distr,))
    mun_distr_id = cur.fetchone()[0]

    for zbs in json_mun_distr_childs[mun_distr] :
        cur.execute('SELECT id FROM ZBS WHERE name = ? LIMIT 1', (zbs,))
        zbs_id = cur.fetchone()[0]
        cur.execute('''INSERT OR IGNORE INTO ZBS_fathers (zbs_id, mun_distr_id)
                    VALUES (?,?)''', (zbs_id, mun_distr_id))
        conn.commit()


### Create and fill table for political areas (Municipios y Distritos) related with its fathers
fname = 'zbs_childs.json'
str_zbs_childs = open(fname).read()
json_zbs_childs = json.loads(str_zbs_childs)

cur.execute('DROP TABLE IF EXISTS MunDistr_fathers')
cur.execute('''CREATE TABLE MunDistr_fathers (mun_distr_id INTEGER UNIQUE, zbs_id INTEGER)''')

for zbs in json_zbs_childs :
    cur.execute('SELECT id FROM ZBS WHERE name = ? LIMIT 1', (zbs,))
    zbs_id = cur.fetchone()[0]

    for mun_distr in json_zbs_childs[zbs] :
        cur.execute('SELECT id FROM MunDistr WHERE name = ? LIMIT 1', (mun_distr,))
        mun_distr_id = cur.fetchone()[0]
        print(mun_distr, zbs)
        print(mun_distr_id, zbs_id)
        cur.execute('''INSERT OR IGNORE INTO MunDistr_fathers (mun_distr_id, zbs_id)
                    VALUES (?,?)''', (mun_distr_id, zbs_id))
        conn.commit()

cur.close()
