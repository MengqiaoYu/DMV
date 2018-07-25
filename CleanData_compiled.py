import csv
import numpy as np
import random
import os
import datetime
import pandas as pd
import logging

def find_records(data, data_header, addr_set = '', name_set = '', vin_set = ''):
    """

    :param data: dataset
    :type data: list of lists
    :param data_header: column names of the dataset
    :type data_header: list
    :param addr_set: desired address set
    :type addr_set: list of lists
    :param name_set: desired name set
    :type name_set: list
    :param vin_set: desired vin set
    :type vin_set: list
    :return: all the satisfying records
    :rtype: list of lists
    """
    ret = []

    if name_set == '': # implies only need to find address
        add_count = {str(key): 0 for key in addr_set}
        for i, item in enumerate(data):
            addr_curr = item[data_header.index('address')]
            if addr_curr in addr_set:
                add_count[str(addr_curr)] +=1
                # Delete the addresses with more than 5 vehicles
                if add_count[str(addr_curr)] <= 5:
                    ret.append(item)
                else:
                    addr_set.remove(addr_curr)
    else:
        for i, item in enumerate(data):
            name_curr = item[data_header.index('name')]
            addr_curr = item[data_header.index('address')]
            vin_curr = item[data_header.index('vin')]
            # print name_curr in name_set
            # print addr_curr in addr_set
            # print vin_curr in vin_set
            if (name_curr in name_set) and (addr_curr in addr_set or vin_curr in vin_set):
                ret.append(item)
    return ret

def find_unique_name(data, data_header):
    """

    :param data: dataset
    :type data: list of lists
    :param data_header: header
    :type data_header: list
    :return: all the unique names
    :rtype: lists
    """
    return list(set([item[data_header.index('name')] for item in data]))

def find_unique_vin(data, data_header):
    """

    :param data:
    :type data:
    :param data_header:
    :type data_header:
    :return: all the unique vin
    :rtype: list
    """
    return list(set([item[data_header.index('vin')] for item in data]))

def find_unique_addr(data, data_header):
    """

    :param data:
    :type data:
    :param data_header:
    :type data_header:
    :return: all the unique address (combo of address and city)
    :rtype: list of lists
    """
    # return [list(x) for x in set(tuple(x[data_header.index('address')]) for x in data)]
    return list(set([item[data_header.index('address')] for item in data]))

def pick_random_addr(data_init, data_header, init_year, sample_size):
    """
    randomly pick sample_size addresses in the first year of the database and extract all the records
    :param data:
    :type data:
    :param data_header:
    :type data_header:
    :return:
    :rtype:
    """
    ### Find all the unique addresses in 2008 and random pick 100
    print ("Now find all unique addresses in %d..." % init_year)
    addr_init_set = find_unique_addr(data_init, data_header)
    print ("There are %d unique addresses in the first year DMV dataset." % len(addr_init_set))
    print ("Randomly pick %d address from the set of all address in %d..." % (sample_size,  init_year))
    addr_init_sample = random.sample(addr_init_set, sample_size)
    print ("Random pick over.")
    return addr_init_sample

def remove_commercial(data, data_header):
    commercial_name = ["SERVICE", "AUTOMOBILE", "MUTUAL", "INSURANCE", "COMPANY",
                       "INS CO", "L.P", "FINANCING", "ELECTRIC", "ENTERPRIS", "TRUST",
                       "CORPORATION", " INC"]
    index_to_delete = []
    for i, item in enumerate(data):
        if any(name in item[data_header.index('name')] for name in commercial_name):
            index_to_delete.append(i)
    for i in sorted(index_to_delete, reverse=True):
        del data[i]
    return data

def simplepick_random_addr(data_init, data_header, sample_size):
    """
    Random pick sample_size observations and return the unique addresses
    (save a lot of time compared with pick_random_addr)
    """
    data_sample = random.sample(data_init, sample_size)
    data_sample_filter = remove_commercial(data_sample, data_header)
    addr_sample_set = find_unique_addr(data_sample_filter, data_header)
    logger.info("There are %d unique effective addresses in the first year." % len(addr_sample_set))
    return addr_sample_set

def forward_track_hh(year, data_curr, header_curr, addr_prev, name_prev = '', vin_prev = ''):
    """
    if first year, track households for this year based on picked addresses;
    if other years, track households for this year based on previous year's addr, vin, and names.
    :return: 4 items: all the records, name, add and vin info
    :rtype:
    """
    ### Find all the records, names in the picked sample_size addresses since the first year

    logger.info("Find the records related with the %d households in %d ..." % (year - 1, year))
    data_sample = find_records(data = data_curr, data_header = header_curr, addr_set = addr_prev, name_set=name_prev, vin_set=vin_prev)
    addr_sample = find_unique_addr(data_sample, header_curr)
    data_sample = find_records(data = data_curr, data_header = header_curr, addr_set = addr_sample, name_set='', vin_set='')
    name_sample = find_unique_name(data_sample, header_curr)
    vin_sample = find_unique_vin(data_sample, header_curr)
    logger.info('There are %d households, %d names and %d vehicles in these households in %d.\n' \
          % (len(addr_sample), len(name_sample), len(vin_sample), year))

    # print 'We can observe that there are %s individuals disappear in 2009, and %d individuals are added to these household in 2009.\n' 
    # % (len(set(name_2008_sample) - set(name_2009_sample)), len(set(name_2009_sample) - set(name_2008_sample)))

    return data_sample, name_sample, addr_sample, vin_sample

def is_addr_intersection(set1, set2):
    """
    :param set1: address(es) in previous year
    :param set2: address(es) in previous year
    :return: The number of overlapping address(es)
    :rtype: integer
    """
    set1 = set(set1)
    set2 = set(set2)
    return len(set1 & set2)

def init_household_dict(data_init, year_init):
    """
    :return: the household info for the first year. A household is defined based on one address in the first year.
    """

    addr_init = [item[header_hh.index('address')] for item in data_init]
    addr_init_set = list(set(addr_init))

    household_dict_init = [None] * len(addr_init_set)

    for item in data_init:
        for i, addr in enumerate(addr_init_set):
            if item[header_hh.index('address')] == addr:
                item.append(i)
                if household_dict_init[i] is None:
                    household_dict_init[i]  = {'name': [item[header_hh.index('name')]],
                                               'address': item[header_hh.index('address')],
                                               'vin': [item[header_hh.index('vin')]],
                                               'id': [item[header_hh.index('id')]],
                                               'household_id': str(i) + ',',
                                               'year': item[header_hh.index('year')]}
                                               # 'vmt': item[header_hh.index('vmt')]}
                else:
                    household_dict_init[i]['name'].append(item[header_hh.index('name')])
                    household_dict_init[i]['vin'].append(item[header_hh.index('vin')])
                    household_dict_init[i]['id'].append(item[header_hh.index('id')])
                    # household_dict_init[i]['vmt'].append(item[header.index('vmt')])

    logger.info('There are %d households in ' % len(household_dict_init) + str(year_init) + ' records.\n')
    return household_dict_init, len(household_dict_init) - 1

def calc_household_dict(data_curr, household_dict_prev, year_curr, num_household_prev):
    """
    :param data_curr: current dataset eg: data_2009
    :type data_curr:
    :param household_dict_prev: previous year household info
    :type household_dict_prev: list of dictionaries
    :param year_curr: current year eg: 2009
    :type year_curr:
    :return: household info for current year
    :rtype: list of dictionaries
    """

    addr_curr = [item[header_hh.index('address')] for item in data_curr]
    addr_curr_set = list(set(addr_curr))
    household_dict_curr = [None] * len(addr_curr_set)

    # First define all the households in 2009
    for item in data_curr:
        for i, addr in enumerate(addr_curr_set):
            if item[header_hh.index('address')] == addr:
                item.append(i)
                # print (item)
                if household_dict_curr[i] is None:
                    household_dict_curr[i]  = {'name': [item[header_hh.index('name')]],
                                               'address': item[header_hh.index('address')],
                                               'vin': [item[header_hh.index('vin')]],
                                               'id': [item[header_hh.index('id')]],
                                               'household_id': '',
                                               'year': item[header_hh.index('year')]}
                else:
                    household_dict_curr[i]['name'].append(item[header_hh.index('name')])
                    household_dict_curr[i]['vin'].append(item[header_hh.index('vin')])
                    household_dict_curr[i]['id'].append(item[header_hh.index('id')])
                    # household_dict_curr[i]['vmt'].append(item[header.index('vmt')])
    logger.info('There are %d households in ' % len(household_dict_curr) + str(year_curr) + ' records.\n')

    # Now combine the households in 2009 and 2008, if not exist in 2008, give a new household id.
    # num_household_prev = len(household_dict_prev)
    for item_curr in household_dict_curr:
        name_curr = item_curr['name']
        addr_curr = item_curr['address']
        vin_curr = item_curr['vin']
        for item_prev in household_dict_prev:
            if len(set(name_curr) & set(item_prev['name'])) != 0 \
                and (addr_curr == item_prev['address']
                or len(set(vin_curr) & set(item_prev['vin'])) != 0):
                item_curr['household_id'] += (str(item_prev['household_id']))

        if item_curr['household_id']=='':
            item_curr['household_id'] = str(num_household_prev + 1) + ','
            num_household_prev +=  1

    return household_dict_curr, num_household_prev

def extract_geoid(data, header):
    county_code = data[header.index('COUNTYCODE')]
    while len(county_code) < 3:
        county_code = '0' + county_code
    tract_code = data[header.index('CENSUS_TRK')]
    while len(tract_code) < 6:
        tract_code = '0' + tract_code
    blockgroup_code = data[header.index('BLKGRP')]
    geoid = '48' + county_code + tract_code + blockgroup_code # 12 string
    return geoid

def find_avg_policy(date, datafile):
    """
    return the average value of the gas price/unemployment rate in the past 12 months
    """
    policy_value = []
    for d in date:
        datee = datetime.datetime.strptime(d, "%m/%Y") #01/03/2008
        _month, _year = datee.month, datee.year
        index_datafile = (_year - 2002) * 12 + _month - 1
        policy_value.extend(datafile[index_datafile - 12 : index_datafile])
    return np.average(policy_value)


### Process header
# header_dmv = "DATE ST_TIME	VIN	ODOMETER	VEHAGE	VMTCORR	PREVDATE	PREVODO	ODODIFF	DATEDIFF	VMT	VMT1000	VMT10002	VMTGRP	BADVMT	MY	MAK2	MM2	CG	VTYP	VTYP2	VTYP3	YEAR	VID_MY	VID_CYCLE	VID_COUNTY	PREPLTNO	REGPLTNO	REGEXPYR	REGEXPMO	OWNERNAME1	REG_MY	REG_ODO	REG_DATE	ADD_F	CITY_F	STATE_F	ZIP_F	REC_TYPE	COUNTYCODE	CENSUS_TRK	CENSUS_BLK	MONSBTWN2	MONTH	BLKGRP	ZIP5_F	DEN_ZIP	INC_ZIP	POP_ZIP	POP_BLK	INC_BLK	DEN_BLK"
logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

header_dmv = "ADD_F BADVMT BLKGRP CENSUS_BLK CENSUS_TRK CG CITY_F COUNTYCODE DATEDIFF DEN_BLK DEN_ZIP INC_BLK INC_ZIP MAK2 MM2 MONSBTWN2 MONTH MY ODODIFF ODOMETER OWNERNAME1 POP_BLK POP_ZIP PREVDATE PREVODO REC_TYPE REG_DATE REG_MY REG_ODO REGEXPMO REGEXPYR REGPLTNO ST_TIME STATE_F VEHAGE VID_COUNTY VID_CYCLE VID_DATE VID_LICENSE VID_MY VIN VMT VMT1000 VMT10002 VMTCORR VMTGRP VTYP VTYP2 VTYP3 YEAR ZIP5_F"
header_dmv = header_dmv.split()
# print ("The number of columns in the raw dataset is %d " % len(header_dmv)) # 51
header_hh = ['name', 'address', 'vin', 'id', 'year']

### Revise the dataset
# outputfile = open("/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData/formy4_revised.txt", 'w')
# writer = csv.writer(outputfile, delimiter = '@')
# temp = []
# with open("/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData/formy4.txt", 'r', encoding = "ISO-8859-1") as inputfile:
#     for row in csv.reader(inputfile, delimiter = '@'):
#         if len(row) == len(header_dmv):
#             if row[-2] is not '.':
#                 writer.writerow(row)
#                 temp = []
#             continue
#         temp = temp + row
#         if len(temp) == len(header_dmv):
#             if temp[-2] is not '.':
#                 writer.writerow(temp)
#                 temp = []

### Initialize some variables.
year_tot = [2005, 2006, 2007, 2008, 2009, 2010, 2011]
first_year = year_tot[0]
data_raw_tot = {} # Store each dmv record only with 5 important variables.


### Load data
logger.info("Now load data based on year (only 5 important columns are extracted)...")
for y in year_tot:
    data_raw_tot[y] = []
id = 0

with open("/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData/formy4_revised.txt", 'r', encoding = "ISO-8859-1") as inputfile:
    for row in csv.reader(inputfile, delimiter = '@'):
        if len(row) == 1:
            row = row[0]
        try:
            data_raw_tot[int(row[header_dmv.index('YEAR')])].append([
                row[header_dmv.index('OWNERNAME1')],
                hash(row[header_dmv.index('ADD_F')] + ' '+ row[header_dmv.index('CITY_F')]),
                hash(row[header_dmv.index('VIN')]),
                id,
                row[header_dmv.index('YEAR')]
            ])
            id += 1
        except Exception:
            logger.debug(row)
            id += 1
logger.info("Load data over. There are %d rows in formy4.txt.\n" %id)

name_tot = {}
addr_tot = {}
vin_tot = {}
data_sample_tot = {} # Store dmv records related with 200 sample addresses with 5 important variables.
all_household_dicts = {} # Store all hh records related with 200 sample addresses.
num_sample = 150
logger.info("--------Now randomly pick %d addresses from %d...--------\n" % (num_sample, first_year) )
addr_tot[first_year] = simplepick_random_addr(data_init=data_raw_tot[year_tot[0]],
                                              data_header=header_hh,
                                              sample_size=num_sample)
logger.info("Extract info of these %d addresses..." %num_sample)
data_sample_tot[first_year] = find_records(data = data_raw_tot[first_year],
                                           data_header = header_hh,
                                           addr_set = addr_tot[first_year],
                                           name_set='',
                                           vin_set='')
name_tot[first_year] = find_unique_name(data_sample_tot[first_year], header_hh)
vin_tot[first_year] = find_unique_vin(data_sample_tot[first_year], header_hh)
logger.info('There are %d records, %d names, %d vehicles in these %d households in %d.\n' \
      % (len(data_sample_tot[first_year]), len(name_tot[first_year]),
         len(vin_tot[first_year]), len(addr_tot[first_year]), first_year))

### Find records in the next few years
logger.info("-----------Execute forward tracking algorithm...----------")
for year_curr in year_tot[1:]:
    data_sample_tot[year_curr], name_tot[year_curr], addr_tot[year_curr], vin_tot[year_curr] = \
        forward_track_hh(year=year_curr, data_curr=data_raw_tot[year_curr], header_curr=header_hh,
                         addr_prev=addr_tot[year_curr-1], name_prev=name_tot[year_curr-1], vin_prev=vin_tot[year_curr-1])


### Recover all the records
logger.info("----------Execute backward recovering algorithm...--------")
name_new_tot = []
vin_new_tot = []
for year_curr in reversed(year_tot[1:]):
    name_new_tot += set(name_tot[year_curr]) - set(name_tot[year_curr - 1])
    logger.debug(len(name_new_tot))
    name_new_find = list(set(name_new_tot) - set(name_tot[year_curr - 1]))
    logger.debug(len(name_new_find))
    vin_new_tot += set(vin_tot[year_curr]) - set(vin_tot[year_curr - 1])
    vin_new_find = list(set(vin_new_tot) - set(vin_tot[year_curr - 1]))
    data_new_curr = find_records(data = data_raw_tot[year_curr - 1],
                                 data_header = header_hh,
                                 addr_set = [],
                                 name_set=name_new_find,
                                 vin_set=vin_new_find)
    # print (data_new_curr)
    logger.info("There are %d newly added records in %d." %(len(data_new_curr), year_curr-1))
    data_sample_tot[year_curr-1] += data_new_curr

data_raw_tot = [] # to save memory
logger.info("Save the sample records.\n")
with open('/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData/Sample_records_7year_sample8.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(header_hh)
    for y in year_tot:
        writer.writerows(data_sample_tot[y])


### Give household id to all the records in the first year
logger.info("-----------Now let's build household dataset.------------")
logger.info("Now let's initialize household id to records in the first year (%d)...\n" % first_year)
all_household_dicts[first_year], household_last_id = init_household_dict(data_sample_tot[first_year], first_year)

### Give household id to all the records in the next few years.
for year_curr in year_tot[1:]:
    logger.debug("Now let's allocate household id to records in %d...\n" % year_curr)
    all_household_dicts[year_curr], household_last_id = calc_household_dict(data_sample_tot[year_curr],
                                                                            all_household_dicts[year_curr - 1],
                                                                            year_curr,
                                                                            household_last_id)

# Write household info to file
outdir_name = '/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData'
outfile_name = 'household_7years_sample8.csv'
logger.info("Saving the household info to directory: %s" % outdir_name)
with open(os.path.join(outdir_name, outfile_name), 'w') as f:
    dict_writer = csv.DictWriter(f, all_household_dicts[first_year][0].keys())
    dict_writer.writeheader()
    for year, household_dict in all_household_dicts.items():
        dict_writer.writerows(household_dict)


###--------- Convert into individual database----------------###

### Find all the records in the dmv database
logger.info("-----------Now let's build individual dataset.------------")
logger.info("Load all useful datasets\n")
hh_index_in_rawdata = []
ind_dmv_records = []

for year, household_dict in all_household_dicts.items():
    for item in household_dict:
        hh_index_in_rawdata.extend(item['id'])

# i = 0
# with open("/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData/formy4_revised.txt", 'r', encoding = "ISO-8859-1") as inputfile:
#     for row in csv.reader(inputfile, delimiter = '@'):
#         if i in hh_index_in_rawdata:
#             row.append(str(i))
#             ind_dmv_records.append(row)
#         i += 1
# header_dmv_new = header_dmv + ['ID']

### Speed up the I/O process
skip_id = list(set(range(id)) - set(hh_index_in_rawdata))

ind_dmv_records = pd.read_csv('/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData/formy4_revised.txt', sep="@", header=None, skiprows = skip_id)
ind_dmv_records.columns = header_dmv
ind_dmv_records['ID'] = sorted(hh_index_in_rawdata)
logger.debug(ind_dmv_records.head())
ind_dmv_records = ind_dmv_records.values


### Load the clustering results file
bg_data = []
with open('/Users/MengqiaoYu/Desktop/Research_firstApproach/HMM/bg_att_all_labeled_0715.csv', 'rU') as inputfile:
    for row in csv.reader(inputfile):
        bg_data.append(row)
bg_header = bg_data[0]
bg_data = bg_data[1:]
bg_geoid = [item[bg_header.index('GEOID10')] for i, item in enumerate(bg_data)]

### Load the gas price file
df_gp = pd.read_csv('/Users/MengqiaoYu/Desktop/Research_firstApproach/HMM/GP_test.csv', dtype = float)
gp_data = df_gp.values

### Load unemployment rate file
df_ue = pd.read_csv('/Users/MengqiaoYu/Desktop/Research_firstApproach/HMM/UE_test.csv', dtype = float)
ue_data = df_ue.values

### Load VIN decoder
## Data processing: only extract comb08epa from raw vin decoder dataset
# header_decoder = "VIN MAKE_VIN MODEL_MOD_VIN YEAR_VIN FUELTYPE1_VIN DRIVE_MOD_VIN2 DISPL_MOD_VIN CYLINDERS_VIN WEIGHT_VIN CITY08_EPA COMB08_EPA HIGHWAY08_EPA BODYCABTYPE_VIN DOORS_VIN VTYP_EPA".split(' ')
# outputfile = open("/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData/myvindecode_revised.txt", 'w')
# writer = csv.writer(outputfile, delimiter = ',')
# temp = []
# with open("/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData/myvindecode.txt", 'r') as inputfile:
#     for row in csv.reader(inputfile, delimiter = ','):
#         writer.writerow([row[header_decoder.index('VIN')], row[header_decoder.index('COMB08_EPA')]])
df_decoder = pd.read_csv('/Users/MengqiaoYu/Desktop/Research_firstApproach/CleanData/myvindecode_revised.txt', header=None)
decoder_data = {item[0]: item[1] for item in df_decoder.values}

### Change to ind dataset
logger.info(" Load Data over. Now let's convert to ind dataset.")
# TODO: add household vehicle portfolio
hh_id = [int(item[-1]) for item in ind_dmv_records]
ind_records = []
header_ind = "name year vmt_tot veh_num vmt_avg geoid cluster_id address gas_price ue_rate_scaled vin mpg"
header_ind = header_ind.split()
for year, household_dict in all_household_dicts.items():
    for hh_curr in household_dict:
        hh_veh_num = 0
        # hh_name_curr = list(set(hh_curr['name']))
        hh_name_curr = []
        hh_vin_curr = []
        hh_vmt_curr = 0
        hh_date = []
        hh_mpg_curr = []
        for i in hh_curr['id']:
            dmv_record = ind_dmv_records[hh_id.index(i)]

            # Reverse hash name, vin
            hh_name_curr.append(dmv_record[header_dmv.index('OWNERNAME1')])
            vin_curr = dmv_record[header_dmv.index('VIN')]
            hh_vin_curr.append(vin_curr)

            # Find mpg for each vin
            try:
                hh_mpg_curr.append(int(decoder_data[vin_curr]))
            except Exception:
                logger.debug("Cannot find mpg for vehicle %s" %vin_curr)
                hh_mpg_curr = 'NaN'
                break

            # Yield total VMT and num of vehs in hh. If one bad VMT, label NaN
            if dmv_record[header_dmv.index('VMT')] == '.' or \
                            dmv_record[header_dmv.index('VMT')] == '.' or \
                            float(dmv_record[header_dmv.index('VMT')]) <= 0:
                hh_vmt_curr += 0
            else:
                hh_vmt_curr += float(dmv_record[header_dmv.index('VMT')])
                hh_veh_num += 1

            # Yield location cluster. If cannot find a corresponding cluster, label NaN
            hh_add = [dmv_record[header_dmv.index('ADD_F')], dmv_record[header_dmv.index('CITY_F')]]
            hh_geoid = extract_geoid(dmv_record, header_dmv)
            try:
                hh_cluster = bg_data[bg_geoid.index(hh_geoid)][bg_header.index("cluster_label")].strip('[]')
            except Exception:
                logger.debug("Cannot find cluster for geoid %s" %hh_geoid)
                hh_cluster = 'NaN'
                break

        # Yield a list of inspection time for all the vehs in the hh
        hh_date.append(str(dmv_record[header_dmv.index('MONTH')]) + '/' + str(dmv_record[header_dmv.index('YEAR')]))
        hh_gasPrice = find_avg_policy(hh_date, gp_data)
        hh_ueRate = find_avg_policy(hh_date, ue_data)

        # Save the data
        hh_name_curr = list(set(hh_name_curr))
        if hh_vmt_curr <= 1000 or hh_cluster == 'NaN' or hh_mpg_curr == 'NaN':
            continue
        if len(hh_name_curr) == 1:
                ind_records.append([hh_name_curr[0], year, hh_vmt_curr, float(hh_veh_num),
                                    hh_vmt_curr / float(hh_veh_num), hh_geoid, hh_cluster, hh_add,
                                    hh_gasPrice, hh_ueRate * 100, hh_vin_curr, hh_mpg_curr])
        else:
            for n in hh_name_curr:
                ind_records.append([n, year, hh_vmt_curr, float(hh_veh_num),
                                    hh_vmt_curr / float(hh_veh_num), hh_geoid, hh_cluster, hh_add,
                                    hh_gasPrice, hh_ueRate * 100, hh_vin_curr, hh_mpg_curr])


#### Delete bad records
# index_to_delete = []
# for i, item in enumerate(ind_records):
#     if item[header_ind.index('cluster_id')] == 'NaN':
#         index_to_delete.append(i)
# for i in sorted(index_to_delete, reverse=True):
#     del ind_records[i]

ind_names = [item[header_ind.index('name')] for item in ind_records]
result_for_model = []
for item in ind_records:
    if ind_names.count(item[header_ind.index('name')]) == len(year_tot):
        result_for_model.append(item)


with open('/Users/MengqiaoYu/Desktop/Research_firstApproach/HMM/data_0617_7year_sample8.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(header_ind)
    writer.writerows(result_for_model)
# nearly 60% households are preserved.

logger.info("---------------------THE END-------------------------------")