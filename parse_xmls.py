import os
import json
import signal
import random
import psycopg2
import argparse
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from multiprocessing import Pool
from dotenv import dotenv_values

from utils.qc_roll_mapping import *

# Read in the database configuration from a .env file
DB_CONFIG = dotenv_values(".env")

def launch_jobs(input_folder, num_workers):

    # Split the XMLs evenly between the workers
    splits = split_xmls_between_workers(input_folder, num_workers)

    # launch a process pool mapping the parsing function and the XMLs
    with Pool(processes=num_workers) as pool:
        pool.map(parse_xmls, splits)
        # for _ in pool.imap_unordered(parse_xmls, splits):
        #     pass

def split_xmls_between_workers(input_folder, num_workers):

    all_files = list(input_folder.iterdir())

    # Shuffle the list since they don't have all the same number of items
    # A better way to do this would be to have counts of items in each file
    # a split so that each worker gets approximately the same amount of items
    random.shuffle(all_files)
    
    # Some python magic to even split the files
    return [all_files[i::num_workers] for i in range(num_workers)]


def parse_xmls(xml_files):
    pid = os.getpid()
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Establish worker DB connection
    # conn_string = 'postgresql://postgres:Gr33ssp0st44\$\$@localhost:5432/proll'
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor()

    units_per_muni = {}

    total_units = 0

    for xml_file in xml_files:

        print(f'{pid}: processing {xml_file}')

        xml_file = open(xml_file, 'r', encoding='utf-8')
        content = xml_file.readlines()
        content = "".join(content)
        xml = BeautifulSoup(content, 'lxml')
        
        unit_data = {}

        # Municipal code is at the top
        code_muni = xml.find('rlm01a').text

        year_entered = int(xml.find('rlm02a').text)

        # Iterate over the units in the file
        for i, unit in enumerate(xml.find_all('rluex')):
            
            # Commit writes every 1000 units
            if i % 1000 == 0 and i > 0:
                conn.commit()
                print(f'{pid}:\tOn unit {i}')

            unit_data = {}
            unit_data['muni'] = code_muni
            unit_data['year'] = year_entered

            # RL0104 - we'll use it to create the MAT18 and ID_PROVINC used in the GIS data
            # Do this first to check if the unit has already been entered and skip work
            rl0104 = unit.find('rl0104')
            mat18 = generate_mat18(rl0104)
            unit_data['mat18'] = mat18
            unit_data['id'] = code_muni + mat18

            cursor.execute("""SELECT """)

            # RL0101: Unit Identification Fields
            # Every unit must have at least RL0101Gx, so RL0101 will always be present
            # They made so that multiple RL0101x could be present, but in practice there's always a single one
            rl0101x = unit.find('rl0101x')
            unit_data['address'] = generate_address(rl0101x)
                        
            # Keep the apt number separate from the street address 
            # so we can merge MURBs that are disaggregated into indiviudal units
            unit_data['apt_num'] = generate_apt_num(rl0101x)

            # RL0102A 
            unit_data['arrond'] = extract_field_or_none(unit, 'rl0102a')

            # CUBF is mandatory
            unit_data['cubf'] = extract_field_or_none(unit, 'rl0105a', type=int) 

            unit_data['file_num'] = extract_field_or_none(unit, 'rl0106a')
            unit_data['nghbr_unit'] = extract_field_or_none(unit, 'rl0107a')
            

            # RL0201 - Owner Info
            # These are all mandatory fields
            # Most of it is redacted but we can know if the owner is a physical or moral person
            
            rl0201 = unit.find('rl0201')

            # There can be multiple signup dates to the assessment roll
            # Take only the latest one
            max_date = datetime.strptime('1500-01-01', '%Y-%m-%d')
            for rl0201x in rl0201.find_all('rl0201x'):
                
                rl0201gx_tmp = rl0201x.find('rl0201gx').text
                date_time = datetime.strptime(rl0201gx_tmp, '%Y-%m-%d')

                if date_time > max_date:
                    owner_date = rl0201gx_tmp
                    if rl0201x.find('rl0201hx').text == '1':
                        owner_type = 'physical'
                    else:
                        owner_type = 'moral'

            unit_data['owner_date'] = owner_date
            unit_data['owner_type'] = owner_type
            unit_data['owner_status'] = rl0201.find('rl0201u').text
            

            # RL030Xx - Unit Characteristics
            unit_data['lot_lin_dim'] = extract_field_or_none(unit, 'rl0301a', type=float)
            unit_data['lot_area'] = extract_field_or_none(unit, 'rl0302a', type=float)
            unit_data['max_floors'] = extract_field_or_none(unit, 'rl0306a', type=int)
            unit_data['const_yr'] = extract_field_or_none(unit, 'rl0307a', type=int)
            unit_data['const_yr_real'] = extract_field_or_none(unit, 'rl0307b')
            unit_data['floor_area'] = extract_field_or_none(unit, 'rl0308a', type=float)
            # Could resolve this here or later
            unit_data['phys_link'] = extract_field_or_none(unit, 'rl0309a')
            # Could resolve this here or later
            unit_data['const_type'] = extract_field_or_none(unit, 'rl0310a')
            unit_data['num_dwelling'] = extract_field_or_none(unit, 'rl0311a', type=int)
            unit_data['num_rental'] = extract_field_or_none(unit, 'rl0312a', type=int)
            unit_data['num_non_res'] = extract_field_or_none(unit, 'rl0313a', type=int)
            # rl0314 - rl0315 are related to agricultural zones, we ignore them here

            # RL040XX - Value 
            unit_data['apprais_date'] = extract_field_or_none(unit, 'rl0401a')
            unit_data['lot_value'] = extract_field_or_none(unit, 'rl0402a', type=float)
            unit_data['building_value'] = extract_field_or_none(unit, 'rl0403a', type=float)
            unit_data['value'] = extract_field_or_none(unit, 'rl0404a', type=float)
            unit_data['prev_value'] = extract_field_or_none(unit, 'rl0405a', type=float)

            # Write out the current unit to the DB
            cursor.execute(f"""
                INSERT INTO {DB_CONFIG['TABLE_NAME']} 
                    (id, year, muni, arrond, address, apt_num, mat18, cubf, file_num, nghbr_unit, owner_date, owner_type, owner_status, lot_lin_dim, lot_area, max_floors, const_yr, const_yr_real, floor_area, phys_link, const_type, num_dwelling, num_rental, num_non_res, apprais_date, lot_value, building_value, value, prev_value)
                VALUES 
                    (%(id)s, %(year)s, %(muni)s, %(arrond)s, %(address)s, %(apt_num)s, %(mat18)s, %(cubf)s, %(file_num)s, %(nghbr_unit)s, %(owner_date)s, %(owner_type)s, %(owner_status)s, %(lot_lin_dim)s, %(lot_area)s, %(max_floors)s, %(const_yr)s, %(const_yr_real)s, %(floor_area)s, %(phys_link)s, %(const_type)s, %(num_dwelling)s, %(num_rental)s, %(num_non_res)s, %(apprais_date)s, %(lot_value)s, %(building_value)s, %(value)s, %(prev_value)s)
                """, unit_data
            )

        # Flush out the current file's units
        conn.commit()

        units_per_muni[code_muni] = i + 1

        print(f'{pid}:\tTotal: {i + 1} units')

        # Keep count of total number of units seen
        total_units += i + 1

    # Save the number of units per municipality just as extra data
    with open('output/units_per_muni.json', 'w', encoding='utf-8') as outfile:
        json.dump(units_per_muni ,outfile)
        
    print(f'Parsed {total_units} units in total!')



def extract_field_or_none(unit_xml, field_id, type=None):
    if field := unit_xml.find(field_id):
        field = field.text
        if type:
            return type(field)
        return field
    # else
    return None
            

def generate_address(rl0101x):
    address_components = []

    if num_adr_inf := rl0101x.find('rl0101ax'):
        address_components.append(num_adr_inf.text)

    if num_adr_inf_2 := rl0101x.find('rl0101bx'):
        address_components.append(num_adr_inf_2.text)

    if num_adr_sup := rl0101x.find('rl0101cx'):
        address_components.append(num_adr_sup.text)

    if num_adr_sup_2 := rl0101x.find('rl0101dx'):
        address_components.append(num_adr_sup_2.text)

    if way_type := rl0101x.find('rl0101ex'):
        address_components.append(WAY_TYPES[way_type.text])

    if way_link := rl0101x.find('rl0101fx'):
        address_components.append(WAY_LINKS[way_link.text])

    if street_name := rl0101x.find('rl0101gx'):
        address_components.append(street_name.text)

    if cardinal_pt := rl0101x.find('rl0101hx'):
        address_components.append(CARDINAL_POINTS[cardinal_pt.text])

    return " ".join(address_components)


def generate_apt_num(rl0101x):
    apt_num_components = []
    if apt_num := rl0101x.find('rl0101ix'):
        apt_num_components.append(apt_num.text)

    if apt_num_2 := rl0101x.find('rl0101jx'):
        apt_num_components.append(apt_num_2.text)

    if len(apt_num_components) > 0:
        return " ".join(apt_num_components)
    else:
        return None


def generate_mat18(rl0104):
    mat18 = ''
    # rl0104a to c are guaranteed to be present
    mat18 += rl0104.find('rl0104a').text
    mat18 += rl0104.find('rl0104b').text
    mat18 += rl0104.find('rl0104c').text

    # These 3 are optional, if not present, pad with zeros
    if chiffre_auto_verif := rl0104.find('rl0104d'):
        mat18 += chiffre_auto_verif.text
    else:
        mat18 += '0'

    if num_bat := rl0104.find('rl0104e'):
        mat18 += num_bat.text
    else:
        mat18 += '000'

    if num_local := rl0104.find('rl0104f'):
        mat18 += num_local.text
    else:
        mat18 += '0000'

    return mat18


def create_table_if_not_exists():
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_CONFIG['TABLE_NAME']} (
            id CHARACTER(23) PRIMARY KEY,
            year SMALLINT NOT NULL,
            muni CHARACTER(5) NOT NULL,
            arrond TEXT,
            address TEXT NOT NULL,
            apt_num VARCHAR(11),
            mat18 CHARACTER(18) NOT NULL,
            cubf SMALLINT NOT NULL,
            file_num VARCHAR(15),
            nghbr_unit VARCHAR(4),

            owner_date DATE,
            owner_type VARCHAR(8),
            owner_status  CHARACTER(1),

            lot_lin_dim NUMERIC(8, 2),
            lot_area NUMERIC(15, 2),
            max_floors SMALLINT,
            const_yr SMALLINT,
            const_yr_real CHARACTER(1),
            floor_area NUMERIC(8, 1),
            phys_link CHARACTER(1),
            const_type CHARACTER(1),
            num_dwelling SMALLINT,
            num_rental SMALLINT,
            num_non_res SMALLINT,

            apprais_date DATE,
            lot_value INTEGER,
            building_value INTEGER,
            value INTEGER,
            prev_value INTEGER
        );""")
    conn.commit()
    conn.close()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-folder', default='data/xml_2022')
    parser.add_argument('-n', '--num-workers', default=os.cpu_count()-1)
    args = parser.parse_args()

    input_folder = Path(args.input_folder)
    num_workers = args.num_workers

    # Parameter validation
    if not input_folder.exists() or not input_folder.is_dir():
        print(f'Error: bad input directory given')
        exit(-1)

    t0 = datetime.now()
    create_table_if_not_exists()
    launch_jobs(input_folder, num_workers)
    print(f'Finished in {datetime.now() - t0}')