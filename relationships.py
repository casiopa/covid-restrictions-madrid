import json
#import sqlite3

#fname = 'relationships.json'
#str_data = open(fname).read()
#json_data = json.loads(str_data)


fname = 'mun_distr_childs.json'
str_mun_distr_childs = open(fname).read()
json_mun_distr_childs = json.loads(str_mun_distr_childs)

count = 0;
for zbs in json_mun_distr_childs :
    print(zbs)
    count += 1
    print(count)
