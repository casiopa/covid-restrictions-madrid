import urllib.request
import json
import ssl
import sqlite3

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


url_tia_zbs = 'https://datos.comunidad.madrid/catalogo/dataset/b3d55e40-8263-4c0b-827d-2bb23b5e7bab/resource/907a2df0-2334-4ca7-aed6-0fa199c893ad/download/covid19_tia_zonas_basicas_salud_s.json'
url_tia_municipios = 'https://datos.comunidad.madrid/catalogo/dataset/7da43feb-8d4d-47e0-abd5-3d022d29d09e/resource/877fa8f5-cd6c-4e44-9df5-0fb60944a841/download/covid19_tia_muni_y_distritos_s.json'

try:
    str_tia_zbs = urllib.request.urlopen(url_tia_zbs, context=ctx).read()
    str_tia_municipios = urllib.request.urlopen(url_tia_municipios, context=ctx).read()
except:
    print('Unable to retrieve ZBS and/or Municipios TIA data')

try:
    json_tia_zbs = json.loads(str_tia_zbs)
    json_tia_municipios = json.loads(str_tia_municipios)
except:
    print('Unable to parse ZBS and/or Municipios TIA data')

conn = sqlite3.connect('areas.sqlite')
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS ZBS')
cur.execute('''CREATE TABLE ZBS (id INTEGER PRIMARY KEY, name TEXT UNIQUE,
            geometry_code TEXT UNIQUE, tia_14d FLOAT, tia_total FLOAT,
            cases_14d INTEGER, cases_total INTEGER, date TEXT,
            restricted BOOL)''')

# Easy way to get the last information. I should write a function to analyse
# the last date data.
date_chosen = input("Enter date of data:")
if len(date_chosen) < 1 : date_chosen = '2021/02/02 11:07:00'


for zbs in json_tia_zbs['data']:
    if zbs['fecha_informe'] == date_chosen :
        name = zbs['zona_basica_salud']
        geometry_code = zbs['codigo_geometria']
        tia_14d = zbs['tasa_incidencia_acumulada_ultimos_14dias']
        tia_total = zbs['tasa_incidencia_acumulada_total']
        cases_14d = zbs['casos_confirmados_ultimos_14dias']
        cases_total = zbs['casos_confirmados_totales']

        cur.execute('''INSERT OR IGNORE INTO ZBS (name, geometry_code,
                tia_14d, tia_total, cases_14d, cases_total, date) VALUES (?,?,?,?,?,?,?)''',
                (name, geometry_code, tia_14d, tia_total, cases_14d, cases_total, date_chosen))
        conn.commit()


cur.close()
#print(json_tia_zbs)
#print(json_tia_municipios)
