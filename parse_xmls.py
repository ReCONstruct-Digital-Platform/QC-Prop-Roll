import os
import heapq
import signal
import psycopg2
import argparse
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import dotenv_values
from multiprocessing import Pool
from xml.dom.pulldom import parse
from psycopg2.extras import execute_values

from utils.qc_roll_mapping import *

# Read in the database configuration from a .env file
DB_CONFIG = dotenv_values(".env")


def launch_jobs(input_folder: Path, num_workers: int, test: bool = False):

    # Split the XMLs evenly between the workers
    splits = split_xmls_between_workers(input_folder, num_workers, test=test)

    # launch a process pool mapping the parsing function and the XMLs
    with Pool(processes=num_workers) as pool:
        # pool.map(parse_xmls, splits)
        for _ in pool.imap_unordered(parse_xmls, splits):
            pass


def split_xmls_between_workers(input_folder: Path, num_workers: int, test=False):
    """
    Partition the XMLs such that each worker has an approximately equal
    total data size to process. This is because some municipalities (i.e. Montreal)
    have vastly more data than others, and we want to parallelize as best as possible.
    """

    size_per_file = {}
    for xml_file in input_folder.iterdir():
        size_per_file[xml_file] = xml_file.stat().st_size
        
    # Sort sizes in descending order
    size_per_file = dict(sorted(size_per_file.items(), key=lambda x: x[1], reverse=True))
    
    # For testing only, truncate the dictionary to keep only a single file per worker
    # We skip the largest files to reduce the run time of this test
    if test:
        import itertools
        size_per_file = dict(itertools.islice(size_per_file.items(), num_workers, 2 * num_workers))
        print(f'Truncated the input XMLs to {len(size_per_file)}')

    # We iterate over the files, starting from the largest 
    # one and distribute them among workers, always giving 
    # the current to the worker with the least amount of data.
    # Using a min heap, we quickly get the worker with the least amount of data.
    splits = [[] for _ in range(num_workers)]
    worker_heap = [ [0, worker_id] for worker_id in range(num_workers)]
    heapq.heapify(worker_heap)

    # Pop a first worker from the heap
    lowest_worker = heapq.heappop(worker_heap)
    for file, size in size_per_file.items():
        # Get the worker's id
        worker_id = lowest_worker[1]
        # Add the file to the worker's split
        splits[worker_id].append(file)
        # Add the file size to the worker's total size
        lowest_worker[0] += size
        # Push back the worker and pop the new lowest
        lowest_worker = heapq.heappushpop(worker_heap, lowest_worker)

    # Print out results
    print(f'Worker {lowest_worker[1]} will process: {lowest_worker[0]} B')
    while worker_heap:
        worker = heapq.heappop(worker_heap)
        print(f'Worker {worker[1]} will process: {worker[0]} B')

    return splits


def parse_xmls(xml_files):
    pid = os.getpid()
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Establish worker DB connection
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor()

    total_units = 0

    for xml_file in xml_files:

        print(f'{pid}:\tProcessing {xml_file}')

        # We use a streaming XML API for memory efficiency
        # Reading the whole file to build a BeautifulSoup object from it was too much
        event_stream = parse(str(xml_file))

        # Get the municipal code and year entered first
        # Those are applicable to the whole document
        for i, (evt, node) in enumerate(event_stream):
            if evt == 'START_ELEMENT':
                if node.tagName == 'RLM01A':
                    event_stream.expandNode(node)
                    muni_code = node.childNodes[0].nodeValue

                if node.tagName == 'RLM02A':
                    event_stream.expandNode(node)
                    year_entered = node.childNodes[0].nodeValue
                    break
        
        current_units = []
        
        # Go through all the RLUEx tags - each represents a unit
        for i, (evt, node) in enumerate(event_stream):

            if evt == 'START_ELEMENT':
                if node.tagName == 'RLUEx':
                    # Parse until the closing tag
                    event_stream.expandNode(node)
                    unit_xml = BeautifulSoup(node.toxml(), 'lxml')
                    # First get the MAT18 to create the provincial ID
                    mat18 = get_mat18(unit_xml)
                    id = muni_code + mat18

                    # Check if the unit already exists before doing any more work
                    if unit_exists(cursor, id):
                        continue
                    
                    # Start filling the unit values
                    unit_data = {}
                    unit_data['id'] = id
                    unit_data['muni'] = MUNICIPALITIES[f'RL{muni_code}']
                    unit_data['muni_code'] = muni_code
                    unit_data['year'] = year_entered
                    unit_data['mat18'] = mat18
                    current_units.append(parse_unit_xml(unit_xml, unit_data))

                    # # Extract all the information from the unit XML
                    unit_data = parse_unit_xml(unit_xml, unit_data)

            # Print an update and commit latest writes
            if i % 3000 == 0 and i > 0:
                write_out_current_units(current_units, cursor)
                current_units = []
                conn.commit()
                print(f'{pid}:\t\tOn unit {i}\t{xml_file.name}')
                
        # Flush out the current file's units
        write_out_current_units(current_units, cursor)
        conn.commit()

        print(f'{pid}:\tTotal: {i + 1} units')

        # Keep count of total number of units seen
        total_units += i + 1
        
    print(f'{pid}:\tParsed {total_units} units in total!')


def write_out_current_units(current_units, cursor):

    template = """(%(id)s, %(year)s, %(muni)s, %(muni_code)s, %(arrond)s, %(address)s, %(num_adr_inf)s, %(num_adr_inf_2)s, 
        %(num_adr_sup)s, %(num_adr_sup_2)s, %(way_type)s, %(way_link)s, %(street_name)s, %(cardinal_pt)s, 
        %(apt_num)s, %(apt_num_1)s, %(apt_num_2)s, %(mat18)s, %(cubf)s, %(file_num)s, %(nghbr_unit)s, 
        %(owner_date)s, %(owner_type)s, %(owner_status)s, %(lot_lin_dim)s, %(lot_area)s, %(max_floors)s, 
        %(const_yr)s, %(const_yr_real)s, %(floor_area)s, %(phys_link)s, %(const_type)s, %(num_dwelling)s, 
        %(num_rental)s, %(num_non_res)s, %(apprais_date)s, %(lot_value)s, %(building_value)s, %(value)s, 
        %(prev_value)s)"""
    
    execute_values(cursor, f"""INSERT INTO {DB_CONFIG['ROLL_TABLE_NAME']} 
        (id, year, muni, muni_code, arrond, address, num_adr_inf, num_adr_inf_2, num_adr_sup, num_adr_sup_2, 
        way_type, way_link, street_name, cardinal_pt, apt_num, apt_num_1, apt_num_2, mat18, cubf, 
        file_num, nghbr_unit, owner_date, owner_type, owner_status, lot_lin_dim, lot_area, max_floors, 
        const_yr, const_yr_real, floor_area, phys_link, const_type, num_dwelling, num_rental, num_non_res, 
        apprais_date, lot_value, building_value, value, prev_value) VALUES %s ON CONFLICT DO NOTHING""", 
        current_units, template=template)


def get_mat18(unit):
    # RL0104 - we'll use it to create the MAT18 and ID_PROVINC used in the GIS data
    # Do this first to check if the unit has already been entered and skip work
    rl0104 = unit.find('rl0104')
    return generate_mat18(rl0104)

def parse_unit_xml(unit_xml, unit_data):

    # RL0101: Unit Identification Fields
    # Every unit must have at least RL0101Gx, so RL0101 will always be present
    # They made so that multiple RL0101x could be present, but in practice there's always a single one
    rl0101x = unit_xml.find('rl0101x')
    get_address_components_and_resolve(rl0101x, unit_data)

    # Keep the apt number separate from the street address
    # We can use this to merge MURBs that are disaggregated into indiviudal units
    get_apt_num_components(rl0101x, unit_data)

    # RL0102A 
    unit_data['arrond'] = extract_field_or_none(unit_xml, 'rl0102a')

    # CUBF is mandatory
    unit_data['cubf'] = extract_field_or_none(unit_xml, 'rl0105a', type=int) 

    unit_data['file_num'] = extract_field_or_none(unit_xml, 'rl0106a')
    unit_data['nghbr_unit'] = extract_field_or_none(unit_xml, 'rl0107a')
    

    # RL0201 - Owner Info
    # These are all mandatory fields
    # Most of it is redacted but we can know if the owner is a physical or moral person
    rl0201 = unit_xml.find('rl0201')

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
    unit_data['lot_lin_dim'] = extract_field_or_none(unit_xml, 'rl0301a', type=float)
    unit_data['lot_area'] = extract_field_or_none(unit_xml, 'rl0302a', type=float)
    unit_data['max_floors'] = extract_field_or_none(unit_xml, 'rl0306a', type=int)
    unit_data['const_yr'] = extract_field_or_none(unit_xml, 'rl0307a', type=int)
    unit_data['const_yr_real'] = extract_field_or_none(unit_xml, 'rl0307b')
    unit_data['floor_area'] = extract_field_or_none(unit_xml, 'rl0308a', type=float)
    # Could resolve this here or later
    unit_data['phys_link'] = extract_field_or_none(unit_xml, 'rl0309a')
    # Could resolve this here or later
    unit_data['const_type'] = extract_field_or_none(unit_xml, 'rl0310a')
    unit_data['num_dwelling'] = extract_field_or_none(unit_xml, 'rl0311a', type=int)
    unit_data['num_rental'] = extract_field_or_none(unit_xml, 'rl0312a', type=int)
    unit_data['num_non_res'] = extract_field_or_none(unit_xml, 'rl0313a', type=int)
    # rl0314 - rl0315 are related to agricultural zones, we ignore them here

    # RL040XX - Value 
    unit_data['apprais_date'] = extract_field_or_none(unit_xml, 'rl0401a')
    unit_data['lot_value'] = extract_field_or_none(unit_xml, 'rl0402a', type=float)
    unit_data['building_value'] = extract_field_or_none(unit_xml, 'rl0403a', type=float)
    unit_data['value'] = extract_field_or_none(unit_xml, 'rl0404a', type=float)
    unit_data['prev_value'] = extract_field_or_none(unit_xml, 'rl0405a', type=float)

    return unit_data



def unit_exists(cursor, id):
    cursor.execute(f"""SELECT EXISTS (SELECT 1 FROM {DB_CONFIG['ROLL_TABLE_NAME']} WHERE id=%s)""", (id,))
    return cursor.fetchone()[0]


def extract_field_or_none(unit_xml, field_id, type=None):
    if field := unit_xml.find(field_id):
        field = field.text
        if type:
            return type(field)
        return field
    # else
    return None

def extract_field_or_empty_string(unit_xml, field_id):
    if field := unit_xml.find(field_id):
        return field.text
    return ''
            

def get_address_components_and_resolve(rl0101x, unit_data):
    """Add all address subfields and the fully resolved address to unit_data"""
    address_components = []

    if num_adr_inf := rl0101x.find('rl0101ax'):
        unit_data['num_adr_inf'] = num_adr_inf.text
        address_components.append(num_adr_inf.text)
    else:
        unit_data['num_adr_inf'] = None
    
    if num_adr_inf_2 := rl0101x.find('rl0101bx'):
        unit_data['num_adr_inf_2'] = num_adr_inf_2.text
        address_components.append(num_adr_inf_2.text)
    else:
        unit_data['num_adr_inf_2'] = None

    if num_adr_sup := rl0101x.find('rl0101cx'):
        unit_data['num_adr_sup'] = num_adr_sup.text
        address_components.append(num_adr_sup.text)
    else:
        unit_data['num_adr_sup'] = None

    if num_adr_sup_2 := rl0101x.find('rl0101dx'):
        unit_data['num_adr_sup_2'] = num_adr_sup_2.text
        address_components.append(num_adr_sup_2.text)
    else:
        unit_data['num_adr_sup_2'] = None

    if way_type := rl0101x.find('rl0101ex'):
        unit_data['way_type'] = way_type.text
        address_components.append(WAY_TYPES[way_type.text])
    else:
        unit_data['way_type'] = None

    if way_link := rl0101x.find('rl0101fx'):
        unit_data['way_link'] = way_link.text
        address_components.append(WAY_LINKS[way_link.text])
    else:
        unit_data['way_link'] = None

    if street_name := rl0101x.find('rl0101gx'):
        unit_data['street_name'] = street_name.text
        address_components.append(street_name.text)
    else:
        unit_data['street_name'] = None

    if cardinal_pt := rl0101x.find('rl0101hx'):
        unit_data['cardinal_pt'] = cardinal_pt.text
        address_components.append(CARDINAL_POINTS[cardinal_pt.text])
    else:
        unit_data['cardinal_pt'] = None

    unit_data['address'] = " ".join(address_components)


def get_apt_num_components(rl0101x, unit_data):
    apt_num_components = []

    if apt_num_1 := rl0101x.find('rl0101ix'):
        unit_data['apt_num_1'] = apt_num_1.text
        apt_num_components.append(apt_num_1.text)
    else:
        unit_data['apt_num_1'] = None

    if apt_num_2 := rl0101x.find('rl0101jx'):
        unit_data['apt_num_2'] = apt_num_2.text
        apt_num_components.append(apt_num_2.text)
    else:
        unit_data['apt_num_2'] = None

    if apt_num_components:
        unit_data['apt_num'] = " ".join(apt_num_components)
    else:
        unit_data['apt_num'] = None


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


def create_tables_if_not_exists():
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor()
    # Apparently you should never use char(n)
    # even for fixed length character fields and use text or varchar instead
    # https://wiki.postgresql.org/wiki/Don%27t_Do_This#Don.27t_use_char.28n.29
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_CONFIG['ROLL_TABLE_NAME']} (
            id TEXT PRIMARY KEY CHECK(length(id)=23),
            year SMALLINT NOT NULL,
            muni TEXT NOT NULL,
            muni_code TEXT NOT NULL,
            arrond TEXT,
            address TEXT NOT NULL,
            num_adr_inf TEXT,
            num_adr_inf_2 TEXT,
            num_adr_sup TEXT,
            num_adr_sup_2 TEXT,
            way_type TEXT,
            way_link TEXT,
            street_name TEXT,
            cardinal_pt TEXT,
            apt_num TEXT,
            apt_num_1 TEXT,
            apt_num_2 TEXT,
            mat18 TEXT NOT NULL CHECK(length(mat18)=18),
            cubf SMALLINT NOT NULL,
            file_num TEXT,
            nghbr_unit TEXT,

            owner_date DATE,
            owner_type TEXT,
            owner_status TEXT,

            lot_lin_dim NUMERIC(8, 2),
            lot_area NUMERIC(15, 2),
            max_floors SMALLINT,
            const_yr SMALLINT,
            const_yr_real TEXT,
            floor_area NUMERIC(8, 1),
            phys_link TEXT,
            const_type TEXT,
            num_dwelling SMALLINT,
            num_rental SMALLINT,
            num_non_res SMALLINT,

            apprais_date DATE,
            lot_value INTEGER,
            building_value INTEGER,
            value INTEGER,
            prev_value INTEGER
        );""")

    # Create auxiliary table with human readable values of the owner status field
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_CONFIG['OWNER_STATUS_TABLE_NAME']} (
            id TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );""")

    # Create auxiliary table with human readable values of the physical link field
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_CONFIG['PHYS_LINK_TABLE_NAME']} (
            id TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );""")
    
    # Create auxiliary table with human readable values of the construction type field
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_CONFIG['CONST_TYPE_TABLE_NAME']} (
            id TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );""")
    
    conn.commit()
    
    OWNER_STATUSES = (('1', 'landowner'), ('2', 'lessor'), ('3', 'condo owner'), 
        ('4', 'lessor of public land'), ('5', 'tenant of tax-exempt building'), ('6', 'building owner on public land'),
        ('7', 'trailer building owner'), ('8', 'undivided co-owner'), ('9', 'other'))
    
    PHYSICAL_LINKS = [('1', 'single-detached'), ('2', 'semi-detached'), ('3', 'row house'),
        ('4', 'row house (end unit)'), ('5', 'integrated')]
    
    CONSTRUCTION_TYPES = [('1', 'single-storey'), ('2', 'staggered-level'), ('3', 'modular prefab'),
        ('4', 'attic'), ('5', 'full-storey')]
    

    execute_values(cursor, f"""INSERT INTO {DB_CONFIG['OWNER_STATUS_TABLE_NAME']}
        (id, value) VALUES %s ON CONFLICT DO NOTHING""", OWNER_STATUSES)
    
    execute_values(cursor, f"""INSERT INTO {DB_CONFIG['PHYS_LINK_TABLE_NAME']} 
        (id, value) VALUES %s ON CONFLICT DO NOTHING""", PHYSICAL_LINKS)
    
    execute_values(cursor, f"""INSERT INTO {DB_CONFIG['CONST_TYPE_TABLE_NAME']} 
        (id, value) VALUES %s ON CONFLICT DO NOTHING""", CONSTRUCTION_TYPES)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('xml_folder', type=Path, help='Path to folder containing the roll XML files.')
    parser.add_argument('-n', '--num-workers', type=int, default=os.cpu_count()-1, 
                        help="Number of parallel workers. Defaults to one less than the number of CPUs on the machine.")
    parser.add_argument('-t', '--test', action='store_true', help='Run in testing mode on a few XMLs')
    parser.add_argument('-c', '--create-tables', action='store_true', help='Create the tables and exit')
    args = parser.parse_args()

    input_folder = args.xml_folder
    num_workers = args.num_workers
    test = args.test
    create_tables = args.create_tables

    # Parameter validation
    if not input_folder.exists() or not input_folder.is_dir():
        print(f'Error: bad XML directory given')
        exit(-1)

    t0 = datetime.now()
    create_tables_if_not_exists()
    if create_tables:
        exit()
    launch_jobs(input_folder, num_workers, test=test)
    print(f'Finished parsing XMLs in {datetime.now() - t0}')
