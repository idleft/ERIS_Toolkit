#!/bin/python

import os, json, getopt, sys
from asterix_api import appnd_dataset, init_db, create_ext_dataset

work_dir = ''
dn = 'Test0'
tn = 'GeoShapes'
init_ddl = '''
drop dataverse #DATAVERSE if exists;
create dataverse #DATAVERSE;
use dataverse #DATAVERSE;

create type GeoProperty as open{}

create type GeoShapeType as closed{
    rid: int64,
    properties: GeoProperty,
    geometry: {{polygon}}
}

create dataset #DATASET(GeoShapeType)
primary key rid;
'''


def shape2geojson(filename):
    global work_dir
    gj_path = os.path.join(work_dir, filename + '.geojson')
    if os.path.exists(gj_path):
        os.system('rm %s' % gj_path)
    os.system('ogr2ogr -f GeoJSON %s %s' % ( gj_path, os.path.join(work_dir, filename)))
    return filename + '.geojson'


def load_geoj(filename):
    print 'load feas for', filename
    # global work_dir
    print 'in load %s'%work_dir
    ifile = open(os.path.join(work_dir, filename), 'r')
    data = json.load(ifile)
    return data['features']


def geo2adm(geo_dict):
    adm_list = []
    coor = geo_dict['coordinates']
    for scoor in coor:
        ecoor = scoor
        if geo_dict['type'] == 'Polygon':
            adm_list.append(' '.join(','.join(str(y) for y in x) for x in ecoor))
        elif geo_dict['type'] == 'MultiPolygon':
            ecoor = scoor[0]  # save for holes. In holes, the first one is exterior, others are holes
            adm_list.append(' '.join(','.join(str(y) for y in x) for x in ecoor))
    return ','.join(['polygon("%s")' % x for x in adm_list])


def fea2adm(rid, fea):
    geo = "{{" + geo2adm(fea['geometry']) + "}}"
    prop = json.dumps(fea['properties'])
    # rid = 1
    return '{"rid":int64("%d"), "properties":%s, "geometry":%s}' % (rid, prop, geo)


def feas2fadm(fname, feas):
    rid = 0
    ofile = open(os.path.join(work_dir, fname), 'w')
    for rid in range(len(feas)):
        adm_rec = fea2adm(rid, feas[rid])
        ofile.write(adm_rec + '\n')
    ofile.close()


# file name, dataverse name, dataset name
def shape2db(fname, dn, tn):
    global init_ddl
    init_ddl = init_ddl.replace("#DATAVERSE", dn)
    init_ddl = init_ddl.replace("#DATASET", tn)
    init_db(init_ddl)
    gjname = shape2geojson(fname)
    feas = load_geoj(gjname)
    feas2fadm(fname + '.adm', feas)
    print create_ext_dataset(work_dir, fname + '.adm', dn,tn, 'GeoShapeType')


def main(argv):
    global tn, dn, work_dir
    work_dir = os.getcwd()
    try:
        opts, args = getopt.getopt(argv, 'hm:d:t:i:')
    except:
        print 'shape2db -d <dataverse_name> -t <dataset_name> -i <input_file_name.shp>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'asc2adm -d <dataverse_name> -t <dataset_name> -i <input_file_name.shp>'
            sys.exit()
        elif opt == "-d":
            dn = arg
        elif opt == "-t":
            tn = arg
        elif opt == "-i":
            full_path = os.path.abspath(arg)
            parts = os.path.split(full_path)
            work_dir = parts[0]
            fname = parts[1]
    shape2db(fname, dn, tn)


if __name__ == '__main__':
    main(sys.argv[1:])
