import urllib.request
import json
import ssl
import sqlite3
from datetime import datetime

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Madrid health areas: Zonas Básicas de Salud (ZBS)
url_tia_zbs = 'https://datos.comunidad.madrid/catalogo/dataset/b3d55e40-8263-4c0b-827d-2bb23b5e7bab/resource/907a2df0-2334-4ca7-aed6-0fa199c893ad/download/covid19_tia_zonas_basicas_salud_s.json'
# Madrid political areas: Municipios y Distritos
url_tia_mun_distr = 'https://datos.comunidad.madrid/catalogo/dataset/7da43feb-8d4d-47e0-abd5-3d022d29d09e/resource/877fa8f5-cd6c-4e44-9df5-0fb60944a841/download/covid19_tia_muni_y_distritos_s.json'

try:
    str_tia_zbs = urllib.request.urlopen(url_tia_zbs, context=ctx).read()
    str_tia_mun_distr = urllib.request.urlopen(url_tia_mun_distr, context=ctx).read()
except:
    print('Unable to retrieve ZBS and/or Municipios y Distritos TIA data')

try:
    json_tia_zbs = json.loads(str_tia_zbs)
    json_tia_mun_distr = json.loads(str_tia_mun_distr)
except:
    print('Unable to parse ZBS and/or Muni-Distr TIA data')

conn = sqlite3.connect('areas.sqlite')
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS ZBS')
cur.execute('''CREATE TABLE ZBS (id INTEGER PRIMARY KEY, name TEXT UNIQUE,
            geometry_code TEXT UNIQUE, tia_14d FLOAT, tia_total FLOAT,
            cases_14d INTEGER, cases_total INTEGER, date TEXT,
            restricted BOOL)''')
cur.execute('DROP TABLE IF EXISTS MunDistr')
cur.execute('''CREATE TABLE MunDistr (id INTEGER PRIMARY KEY, name TEXT UNIQUE,
            geometry_code TEXT UNIQUE, tia_14d FLOAT, tia_total FLOAT,
            cases_14d INTEGER, cases_total INTEGER, date TEXT,
            restricted BOOL)''')

# Search the last date in json_tia_zbs
format = "%Y/%m/%d %H:%M:%S"
ldatetime = list()
for zbs in json_tia_zbs['data']:
    if datetime.strptime(zbs['fecha_informe'], format) not in ldatetime :
        ldatetime.append(datetime.strptime(zbs['fecha_informe'], format))

last_date_zbs = None
for datetime in ldatetime :
    if last_date_zbs is None or datetime > last_date_zbs : last_date_zbs = datetime

last_date_zbs = str(last_date_zbs).replace('-','/')
#print(last_date_zbs)

# Search the last date in json_tia_mun_distr
ldatetime = list()
for mun_distr in json_tia_mun_distr['data']:
    if datetime.strptime(mun_distr['fecha_informe'], format) not in ldatetime :
        ldatetime.append(datetime.strptime(mun_distr['fecha_informe'], format))

last_date_mun_distr = None
for datetime in ldatetime :
    if last_date_mun_distr is None or datetime > last_date_mun_distr : last_date_mun_distr = datetime

last_date_mun_distr = str(last_date_mun_distr).replace('-','/')
#print(last_date_mun_distr)


# store info for the health areas
for zbs in json_tia_zbs['data']:
    if zbs['fecha_informe'] == last_date_zbs :
        name = zbs['zona_basica_salud']
        geometry_code = zbs['codigo_geometria']

        # Check if the info exists for this area. Not always does.
        if 'tasa_incidencia_acumulada_ultimos_14dias' in zbs :
            tia_14d = zbs['tasa_incidencia_acumulada_ultimos_14dias']
        else : tia_14d = None

        if 'tasa_incidencia_acumulada_total' in zbs :
            tia_total = zbs['tasa_incidencia_acumulada_total']
        else : tia_total = None

        if 'casos_confirmados_ultimos_14dias' in zbs :
            cases_14d = zbs['casos_confirmados_ultimos_14dias']
        else : cases_14d = None

        if 'casos_confirmados_totales' in zbs :
            cases_total = zbs['casos_confirmados_totales']
        else : cases_total = None

        cur.execute('''INSERT OR IGNORE INTO ZBS (name, geometry_code,
                tia_14d, tia_total, cases_14d, cases_total, date) VALUES (?,?,?,?,?,?,?)''',
                (name, geometry_code, tia_14d, tia_total, cases_14d, cases_total, last_date_zbs))
        conn.commit()

# store info for the political areas
for mun_distr in json_tia_mun_distr['data']:
    if mun_distr['fecha_informe'] == last_date_mun_distr :
        name = mun_distr['municipio_distrito']
        geometry_code = mun_distr['codigo_geometria']

        # Check if the info exists for this area. Not always does.
        if 'tasa_incidencia_acumulada_ultimos_14dias' in mun_distr :
            tia_14d = mun_distr['tasa_incidencia_acumulada_ultimos_14dias']
        else : tia_14d = None

        if 'tasa_incidencia_acumulada_total' in mun_distr:
            tia_total = mun_distr['tasa_incidencia_acumulada_total']
        else : tia_total = None

        if 'casos_confirmados_ultimos_14dias' in mun_distr:
            cases_14d = mun_distr['casos_confirmados_ultimos_14dias']
        else : cases_14d = None

        if 'casos_confirmados_totales' in mun_distr :
            cases_total = mun_distr['casos_confirmados_totales']
        else : cases_total = None

        cur.execute('''INSERT OR IGNORE INTO MunDistr (name, geometry_code,
                tia_14d, tia_total, cases_14d, cases_total, date) VALUES (?,?,?,?,?,?,?)''',
                (name, geometry_code, tia_14d, tia_total, cases_14d, cases_total, last_date_mun_distr))
        conn.commit()


cur.close()
