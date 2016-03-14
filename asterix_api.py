#!/bin/python
import os, requests

api_endpoint = 'http://localhost:19002/'
update_endpoint = api_endpoint + 'update'
ddl_endpoint = api_endpoint + 'ddl'
ext_db_sta = 'use dataverse %s;drop dataset TmpExt if exists;create external dataset TmpExt(%s) using localfs(("path"="TMPPath"),("format"="adm"));'
apnd_db_sta = 'use dataverse %s; insert into dataset %s(for $rec in dataset TmpExt return $rec);'
drop_tmp_ddl = 'user dataverse %s, drop dataset TmpExt if exists;'


def init_db(ddl):
    print 'Initialize...'
    response = requests.get(ddl_endpoint, {'ddl': ddl})
    print 'Init', response.status_code


def create_ext_dataset(data_dir, fname, dn, dt):
    cur_ext_path = os.path.join(data_dir, fname)
    cur_ext_ddl = ext_db_sta.replace("TMPPath", 'localhost://' + cur_ext_path)
    response = requests.get(ddl_endpoint, {'ddl': cur_ext_ddl % (dn, dt)})
    return response.status_code == 200


# data dir, fname, dataverse_name, dataset name, datatype
def appnd_dataset(data_dir, fname, dn, tn, dt):
    print 'Try to push: ' + fname
    cur_ext_path = os.path.join(data_dir, fname)
    res_arr = []
    cur_ext_ddl = ext_db_sta.replace("TMPPath", 'localhost://' + cur_ext_path)
    res_arr.append(requests.get(ddl_endpoint, {'ddl': cur_ext_ddl % (dn, dt)}))
    res_arr.append(requests.get(update_endpoint, {'statements': apnd_db_sta % (dn, tn)}))  # push to merge table
    res_arr.append(requests.get(ddl_endpoint, {'ddl': drop_tmp_ddl % dn}))
    return res_arr[0].status_code == 200 and res_arr[0].status_code == 200 and res_arr[0].status_code == 200


def main():
    return


if __name__ == '__main__':
    main()
