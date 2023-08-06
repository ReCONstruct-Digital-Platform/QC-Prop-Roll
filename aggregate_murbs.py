import psycopg2
import psycopg2.extras
from statistics import mean
from collections import Counter
from dotenv import dotenv_values
from psycopg2.extras import execute_values

# Read in the database configuration from a .env file
DB_CONFIG = dotenv_values(".env")

SQL_COPY_TEMPLATE = """(%(id)s, %(lat)s, %(lng)s, %(year)s, %(muni)s, %(muni_code)s, %(arrond)s, %(address)s, 
    %(num_adr_inf)s, %(num_adr_inf_2)s, %(num_adr_sup)s, %(num_adr_sup_2)s, %(way_type)s, %(way_link)s, 
    %(street_name)s, %(cardinal_pt)s, %(apt_num)s, %(apt_num_1)s, %(apt_num_2)s, %(mat18)s, %(cubf)s, 
    %(file_num)s, %(nghbr_unit)s, %(owner_date)s, %(owner_type)s, %(owner_status)s, %(lot_lin_dim)s, 
    %(lot_area)s, %(max_floors)s, %(const_yr)s, %(const_yr_real)s, %(floor_area)s, %(phys_link)s, 
    %(const_type)s, %(num_dwelling)s, %(num_rental)s, %(num_non_res)s, %(apprais_date)s, %(lot_value)s, 
    %(building_value)s, %(value)s, %(prev_value)s)"""

SQL_COPY_DUPLICATES_TO_OTHER_TABLE = f"""INSERT INTO {DB_CONFIG['MURB_DISAG_TABLE_NAME']}
    (id, lat, lng, year, muni, muni_code, arrond, address, num_adr_inf, num_adr_inf_2, num_adr_sup, num_adr_sup_2, 
    way_type, way_link, street_name, cardinal_pt, apt_num, apt_num_1, apt_num_2, mat18, cubf, 
    file_num, nghbr_unit, owner_date, owner_type, owner_status, lot_lin_dim, lot_area, max_floors, 
    const_yr, const_yr_real, floor_area, phys_link, const_type, num_dwelling, num_rental, num_non_res, 
    apprais_date, lot_value, building_value, value, prev_value) VALUES %s ON CONFLICT DO NOTHING"""

SQL_INSERT_AGGREGATED_MURB = f"""INSERT INTO {DB_CONFIG['ROLL_TABLE_NAME']}
        (id, lat, lng, year, muni, muni_code, arrond, address, mat18, cubf, nghbr_unit, owner_date, owner_type, owner_status, 
        lot_lin_dim, lot_area, max_floors, const_yr, const_yr_real, floor_area, phys_link, const_type, num_dwelling, 
        num_rental, num_non_res, apprais_date, lot_value, building_value, value, prev_value) 
    VALUES
        (%(id)s, %(lat)s, %(lng)s, %(year)s, %(muni)s, %(muni_code)s, %(arrond)s, %(address)s, %(mat18)s, %(cubf)s, %(nghbr_unit)s, 
        %(owner_date)s, %(owner_type)s, %(owner_status)s, %(lot_lin_dim)s, %(lot_area)s, %(max_floors)s, 
        %(const_yr)s, %(const_yr_real)s, %(floor_area)s, %(phys_link)s, %(const_type)s, %(num_dwelling)s, 
        %(num_rental)s, %(num_non_res)s, %(apprais_date)s, %(lot_value)s, %(building_value)s, %(value)s, 
        %(prev_value)s) ON CONFLICT DO NOTHING"""

# The parentheses around %s are important here
SQL_DELETE_DUPLICATES = f"""DELETE FROM {DB_CONFIG['ROLL_TABLE_NAME']} WHERE id in (%s)"""


def aggregate_murbs():
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

    # Get all the duplicates
    cursor.execute(f"""SELECT address, muni, lat, lng, count(*) as num_duplicates, sum(num_dwelling) as sum_dwellings 
    FROM {DB_CONFIG['ROLL_TABLE_NAME']} WHERE cubf = 1000 group by lat, lng, address, muni having count(*) > 1 
    ORDER BY count(*) asc""")

    results = cursor.fetchall()
    print(f'{len(results)} duplicates found')

    SQL_GET_DUPLICATES = f"""select * from {DB_CONFIG['ROLL_TABLE_NAME']} 
    WHERE lat = %s and lng = %s and address = %s and muni = %s;"""

    for i, res in enumerate(results):
        
        # get duplicates using the key
        lat, lng, address, muni = res['lat'], res['lng'], res['address'], res['muni']
        
        # Fetch duplicates
        cursor.execute(SQL_GET_DUPLICATES, (lat, lng, address, muni))
        duplicates = cursor.fetchall()

        if len(duplicates) < 1:
            print(f'Error: no duplicates found for {res}')
            continue
        
        # Copy the duplicates to the new table
        execute_values(cursor, SQL_COPY_DUPLICATES_TO_OTHER_TABLE, duplicates, template=SQL_COPY_TEMPLATE)
        conn.commit()

        print(f'Processing MURB {i+1} at {address}\n\t{len(duplicates)} duplicates')
        # Keep each duplicate's ID for deletion
        dupe_ids = []

        # For getting the most frequent
        years = []
        nghbr_units = []
        owner_dates = []
        owner_types = []
        owner_statuses = []
        const_years = []
        const_years_real = []
        apprais_dates = []

        # For summing
        num_rentals = 0
        num_non_res = 0

        # For averaging
        lot_lin_dims = []
        lot_areas = []
        floor_areas = []
        lot_values = []
        building_values = []
        values = []
        prev_values = []

        max_apt_num = 0

        for dupe in duplicates:

            # Gather all IDs to delete them after
            dupe_ids.append((dupe['id'],))

            # Gather most frequent nghbr_unit, owner_type, status, const_yr, yr_real_est, phys_link
            years.append(dupe['year'])
            nghbr_units.append(dupe['nghbr_unit'])
            owner_dates.append(dupe['owner_date'])
            owner_types.append(dupe['owner_type'])
            owner_statuses.append(dupe['owner_status'])
            const_years.append(dupe['const_yr'])
            const_years_real.append(dupe['const_yr_real'])
            apprais_dates.append(dupe['apprais_date'])
            
            # Average these
            if dupe['lot_lin_dim']:
                lot_lin_dims.append(dupe['lot_lin_dim'])
            if dupe['lot_area']:
                lot_areas.append(dupe['lot_area'])
            if dupe['floor_area']:
                floor_areas.append(dupe['floor_area'])
            if dupe['lot_value']:
                lot_values.append(dupe['lot_value'])
            if dupe['building_value']:
                building_values.append(dupe['building_value'])
            if dupe['value']:
                values.append(dupe['value'])
            if dupe['prev_value']:
                prev_values.append(dupe['prev_value'])

            # Sum these
            if num_rental := dupe['num_rental']:
                num_rentals += int(num_rental)
            if non_res := dupe['num_non_res']:
                num_non_res += int(non_res)
            
            # Attempt to cast the lower apt_num to an integer
            if apt_num := dupe['apt_num_1']:
                try:
                    max_apt_num = max(max_apt_num, int(apt_num))
                except ValueError:
                    continue
                
        
        # Initialize the aggregated MURB data
        agg_data = {
            'id': dupe['id'][:-4] + '9999', # We set the last 4 digits of the id to 9999 to recognize them
            'lat': lat,
            'lng': lng,
            'address': address,
            # All of these address fields should be the same for all duplicates, since they
            # were concatenated to form the 'address' field, which is the same for all
            'num_adr_inf': dupe['num_adr_inf'],
            'num_adr_inf_2': dupe['num_adr_inf_2'],
            'num_adr_sup': dupe['num_adr_sup'],
            'num_adr_sup_2': dupe['num_adr_sup_2'],
            'way_type': dupe['way_type'],
            'way_link': dupe['way_link'],
            'street_name': dupe['street_name'],
            'cardinal_pt': dupe['cardinal_pt'],
            'apt_num': dupe['apt_num'],
            'apt_num_1': dupe['apt_num_1'],
            'apt_num_2': dupe['apt_num_2'],

            'muni': muni,
            'mat18': dupe['mat18'][:-4] + '9999', # We set the last 4 digits of the id to 9999 to recognize them
            'phys_link': '1',   # set as detached since we'll be representing the whole building
            'const_type': '5'   # full-storey 
        }

        # Grab the values from the last duplicate - they should be the same for all
        agg_data['cubf'] = dupe['cubf']
        agg_data['arrond'] = dupe['arrond']
        agg_data['muni_code'] = dupe['muni_code']
        agg_data['num_rental'] = num_rentals
        agg_data['num_non_res'] = num_non_res

        agg_data['year'] = Counter(years).most_common(1)[0][0]
        agg_data['nghbr_unit'] = Counter(nghbr_units).most_common(1)[0][0]
        agg_data['owner_date'] = Counter(owner_dates).most_common(1)[0][0]
        agg_data['owner_type'] = Counter(owner_types).most_common(1)[0][0]
        agg_data['owner_status'] = Counter(owner_statuses).most_common(1)[0][0]
        agg_data['const_yr'] = Counter(const_years).most_common(1)[0][0]
        agg_data['const_yr_real'] = Counter(const_years_real).most_common(1)[0][0]
        agg_data['apprais_date'] = Counter(apprais_dates).most_common(1)[0][0]

        agg_data['lot_lin_dim'] = _average_or_none(lot_lin_dims)
        agg_data['lot_area'] = _average_or_none(lot_areas)
        agg_data['floor_area'] = _average_or_none(floor_areas)
        agg_data['lot_value'] = _average_or_none(lot_values)
        agg_data['building_value'] = _average_or_none(building_values)
        agg_data['value'] = _average_or_none(values)
        agg_data['prev_value'] = _average_or_none(prev_values)

        agg_data['max_floors'] = infer_number_of_floors(max_apt_num, lat, lng)

        # May overestimate for some
        agg_data['num_dwelling'] = res['sum_dwellings']

        # Write out the new aggregate MURB
        cursor.execute(SQL_INSERT_AGGREGATED_MURB, agg_data)

        # delete all the duplicates by ID
        execute_values(cursor, SQL_DELETE_DUPLICATES, dupe_ids)
        conn.commit()


def update_aggregated_murbs():
    """
    Function to update the aggregated MURBs if needed
    """
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

    # # Get all the duplicates
    cursor.execute(f"""SELECT address, lat, lng, muni, count(*) as num_duplicates, sum(num_dwelling) as sum_dwellings 
    FROM {DB_CONFIG['MURB_DISAG_TABLE_NAME']} group by lat, lng, address, muni ORDER BY count(*) desc""")

    results = cursor.fetchall()
    print(f'{len(results)} duplicates found')

    SQL_GET_SINGLE_DUPLICATE = f"""select id from {DB_CONFIG['MURB_DISAG_TABLE_NAME']} 
        WHERE lat = %s and lng = %s and address = %s and muni = %s;"""

    SQL_UPDATE_AGGREGATED_MURB = f"""UPDATE {DB_CONFIG['ROLL_TABLE_NAME']} SET lat = %s, lng = %s WHERE id = %s"""

    for i, res in enumerate(results):
        
        # get duplicates using the key
        lat, lng, address, muni = res['lat'], res['lng'], res['address'], res['muni']
        
        # Fetch duplicates
        cursor.execute(SQL_GET_SINGLE_DUPLICATE, (lat, lng, address, muni))
        duplicates = cursor.fetchall()

        for dupe in duplicates:
            # Recreate the ID of the aggregated MURB        
            id = dupe['id'][:-4] + '9999'

            # Ensure we actually have a building in the Roll DB with that ID
            cursor.execute(f"""SELECT EXISTS (SELECT 1 FROM {DB_CONFIG['ROLL_TABLE_NAME']} WHERE id=%s)""", (id,))

            if cursor.fetchone()['exists']:
                break

        if i % 1000 == 0:
            print(f'Updating unit {i}')

        # Update the existing aggregated MURB with the new values
        cursor.execute(SQL_UPDATE_AGGREGATED_MURB, (lat, lng, id))
        conn.commit()


def _average_or_none(arr):
    if arr:
        return round(mean(arr), 2)
    return None


def infer_number_of_floors(max_apt_num, lat, lng):
    num_floors = 1
    # There are only 3 buildings with apt_num > 10,000
    # so we have some hard-coded logic here
    if max_apt_num >= 10_000:
        # 4040 rue de l' ÉCLUSE
        if lat == 46.7174122671 and lng == -71.2773427875:
            num_floors = 3
        else:
            num_floors = 10
    # If between 10,000 and 1,000, consider the first 2 digits to be the floor number
    elif max_apt_num >= 1000:
        num_floors = max_apt_num // 100
    # If between 1 and 1000, consider the first digit to be the floor number
    else:
        num_floors = max_apt_num // 10

    return num_floors


def create_disaggrregated_MURBs_table_if_not_exists():
    """
    Create a new table to hold the disaggregated MURB units, in case we every want to query them again.
    They will then be deleted from the main table, and replaced by their aggregated entries.
    """
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_CONFIG['MURB_DISAG_TABLE_NAME']} (
            id TEXT PRIMARY KEY CHECK(length(id)=23),
            lat NUMERIC(20, 10) NOT NULL,
            lng NUMERIC(20, 10) NOT NULL,
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
    conn.commit()
    conn.close()
    

if __name__ == '__main__':

    create_disaggrregated_MURBs_table_if_not_exists()
    aggregate_murbs()