#!/bin/python

import os, json
from asterix_api import appnd_dataset,init_db

work_dir = '/Volumes/Storage/Home/Xikui/Work/sample_shape'
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
    os.system('ogr2ogr -f GeoJSON %s %s' % (os.path.join(work_dir, filename + '.geojson')
                                            , os.path.join(work_dir, filename)))
    return filename + '.geojson'


def load_geoj(filename):
    print 'load feas for', filename
    # global work_dir
    ifile = open(os.path.join(work_dir, filename), 'r')
    data = json.load(ifile)
    return data['features']


def geo2adm(geo_dict):
    adm_list = []
    coor = geo_dict['coordinates']
    for scoor in coor:
        # ecoor = scoor[0]  # extract one more time
        ecoor = scoor
        if geo_dict['type'] == 'Polygon':
            adm_list.append(' '.join(','.join(str(y) for y in x) for x in ecoor))
        elif geo_dict['type'] == 'MultiPolygon':
            ecoor = scoor[0]  # save for holes. In holes, the first one is exterior, others are holes
            adm_list.append(' '.join(','.join(str(y) for y in x) for x in ecoor))
    return ','.join(['polygon("%s")' % x for x in adm_list])


def fea2adm(rid,fea):
    geo = "{{" + geo2adm(fea['geometry']) + "}}"
    prop = json.dumps(fea['properties'])
    # rid = 1
    return '{"rid":int64("%d"), "properties":%s, "geometry":%s}' % (rid, prop, geo)

def feas2fadm(fname, feas):
    rid = 0
    ofile = open(os.path.join(work_dir,fname),'w')
    for rid in range(len(feas)):
        adm_rec = fea2adm(rid,feas[rid])
        ofile.write(adm_rec+'\n')
    ofile.close()

# file name, dataverse name, dataset name
def shape2db(fname, dn, tn):
    gj_fname = shape2geojson(fname)
    feas = load_geoj(gj_fname)
    feas2fadm(fname+'.adm',feas)
    appnd_dataset(work_dir,fname+'.adm','Test0','GeoShapes','GeoShapeType')


def main():
    global init_ddl
    init_ddl = init_ddl.replace("#DATAVERSE",dn)
    init_ddl = init_ddl.replace("#DATASET",tn)
    init_db(init_ddl)
    fname = 'basins_l2.shp'
    gjname = shape2geojson(fname)
    feas = load_geoj(gjname)
    feas2fadm(fname+'.adm',feas)
    print appnd_dataset(work_dir,fname+'.adm','Test0','GeoShapes','GeoShapeType')
    # print adm_fea
    # print geo2adm(fea['geometry'])


if __name__ == '__main__':
    main()
