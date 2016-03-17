#!/bin/python
import requests
import json
import os, sys, getopt
from asterix_api import init_db, appnd_dataset
from os import listdir

rid = 1;
dn = 'ESRI'
tn = 'Rainfalls'
# http_header = {"content-type": "application/json"}
# api_endpoint = 'http://localhost:19002/'
# update_endpoint = api_endpoint + 'update'
# ddl_endpoint = api_endpoint + 'ddl'
# load_statement = 'use dataverse %s;load dataset %s using localfs(("path"="LOCALPATH"),("format"="adm"));'
# ext_db_sta = 'use dataverse %s;drop dataset TmpExt if exists;create external dataset TmpExt(ERISRecord) using localfs(("path"="TMPPath"),("format"="adm"));'
# apnd_db_sta = 'use dataverse %s; insert into dataset %s(for $rec in dataset TmpExt return $rec);'
# drop_tmp_ddl = 'user dataverse %s, drop dataset TmpExt if exists;'
init_ddl = """drop dataverse %s if exists;
create dataverse %s;
use dataverse %s;
create type ERISRecord as open{rid: int64,
    date: date,
    value: int32,
    center: point
}
create dataset %s(ERISRecord)
primary key rid;
"""


def asc2adm(data_dir, filename):
    global rid;
    cur_date = filename[4:12]
    f = open(os.path.join(data_dir, filename))
    local_db_cache = filename + '.tmp'
    ofile = open(os.path.join(data_dir, local_db_cache), 'w')
    headattr = {}
    lines = f.readlines();
    for iter1 in range(6):
        parts = lines[iter1].replace('\n', '').split()
        headattr[parts[0]] = parts[1]
    xc = float(headattr['xllcorner'])
    yc = float(headattr['yllcorner'])
    nrows = int(headattr['nrows'])
    ncols = int(headattr['ncols'])
    cellsize = float(headattr['cellsize'])
    for iter1 in range(nrows):
        parts = lines[6 + iter1].split()
        for iter2 in range(len(parts)):
            if parts[iter2] != headattr['nodata_value']:
                tuple = ['"rid":%si64' % rid, '"date":date("%s")' % cur_date, '"value":%s' % parts[iter2],
                         '"center":point("%fd,%fd")' % (
                             iter2 * cellsize + cellsize / 2, (nrows - iter1) * cellsize + cellsize / 2)]
                stuple = "{" + ','.join(tuple) + '}' + '\n'
                rid += 1
                ofile.write(stuple)
    print 'Done with ' + filename


def push_to_asterix(data_dir, fname):
    print 'Try to push: ' + fname
    return appnd_dataset(data_dir, fname, dn, tn, 'ERISRecord')


def proc_dir(data_dir):
    curfiles = listdir(data_dir)
    for fname in curfiles:
        if fname.endswith('.asc'):
            if os.path.exists(os.path.join(data_dir, fname + '.tmp')):
                print 'adm exists, import only'
            else:
                print 'generating adm for', fname
                asc2adm(data_dir, fname)
            if push_to_asterix(data_dir, fname + '.tmp'):
                print fname, 'SUCC'
            else:
                print fname, 'Fail'


def direct_push(data_dir):
    files = os.listdir(data_dir)
    for ifile in files:
        if ifile.endswith('.tmp'):
            if push_to_asterix(data_dir, ifile):
                print ifile, 'SUCC'
            else:
                print ifile, 'Fail'


# asc2adm -m append/create -d ESRI -t Rainfalls -i data_dir

def main(argv):
    global tn, dn
    mode = 'create'
    try:
        opts, args = getopt.getopt(argv, 'hm:d:t:i:')
    except:
        print 'asc2adm -m <append/create> -d <dataverse_name> -t <dataset_name> -i <input_dir_path>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'asc2adm -m <append/create> -d <dataverse_name> -t <dataset_name> -i <input_dir_path>'
            sys.exit()
        elif opt in ("-m"):
            mode = arg
        elif opt == "-d":
            dn = arg
        elif opt == "-t":
            tn = arg
        elif opt == "-i":
            data_dir = arg
    if mode == 'create':
        init_db(init_ddl % (dn, dn, dn, tn))
    proc_dir(data_dir)


if __name__ == '__main__':
    main(sys.argv[1:])
    # data_dir = "/Volumes/Storage/Home/Xikui/Work/CHRS/small/"
    # print init_db()  # change to two parameter <dataverse name, dataset name>
    # proc_dir(data_dir)
    # direct_push(data_dir)
    # asc2adm(data_dir, 'CDR_19830101z.asc')
    # print push_to_asterix(data_dir+'adm/')
