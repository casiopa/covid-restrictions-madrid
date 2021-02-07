import urllib.request
import json
import ssl
import sqlite3

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

# Temporal way to get the last information. I should write a function to analyse
# the last date data.
date_chosen = input("Enter date of data:")
if len(date_chosen) < 1 : date_chosen = '2021/02/02 11:07:00'

# store info for the health areas
for zbs in json_tia_zbs['data']:
    if zbs['fecha_informe'] == date_chosen :
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
                (name, geometry_code, tia_14d, tia_total, cases_14d, cases_total, date_chosen))
        conn.commit()

# store info for the political areas
for mun_distr in json_tia_mun_distr['data']:
    if mun_distr['fecha_informe'] == date_chosen :
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
                (name, geometry_code, tia_14d, tia_total, cases_14d, cases_total, date_chosen))
        conn.commit()


cur.close()
#print(json_tia_zbs)
#print(json_tia_municipios)
