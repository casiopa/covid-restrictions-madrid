import json
import sqlite3

# Retrieve restricted_areas.json
fname = 'restricted_areas.json'
str_restricted = open(fname).read()
json_restricted = json.loads(str_restricted)

#print(json_restricted['zbs_restricted'])


conn = sqlite3.connect('areas.sqlite')
cur = conn.cursor()

# Reset column restricted in ZBS table
cur.execute('UPDATE ZBS SET restricted = Null ')
# Update restricted health areas
for zbs in json_restricted['zbs_restricted']:
    try :
        cur.execute('UPDATE ZBS SET restricted = True WHERE name = ?', (zbs,))
        conn.commit()
    except :
        print('Unable to modify', zbs)
        continue


# Reset column restricted in MunDistr table
cur.execute('UPDATE MunDistr SET restricted = Null ')
# Update restricted health areas
for mun_distr in json_restricted['municipios_restricted']:
    try :
        cur.execute('UPDATE MunDistr SET restricted = True WHERE name = ?', (mun_distr,))
        conn.commit()
    except :
        print('Unable to modify', mun_distr)
        continue


cur.close()
