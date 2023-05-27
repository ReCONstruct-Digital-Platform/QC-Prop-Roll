import argparse
import psycopg2
import shapefile
from pathlib import Path
from datetime import datetime
from dotenv import dotenv_values
from pyproj import CRS, Transformer

# Read in the database configuration from a .env file
DB_CONFIG = dotenv_values(".env")

def parse_shapefile(shp_file):
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor()

    with shapefile.Reader(shp_file) as shp:
        
        num_units = len(shp)
        print(f'Shapefile contains {num_units} units')

        for i in range(num_units):

            id = shp.record(i)[0]
            lng, lat = shp.shape(i).points[0]
            # We don't need to transform the coordinates, the point  
            # has lat/lng in NAD83 which is compatbile with WSG84.
            # In QGIS, changing the CRS from NAD83 to WDG84 performs the EPSG-1188 
            # transformation, which we see here https://epsg.io/1188 is a noop.
            # More reading:
            # https://gis.stackexchange.com/questions/304231/converting-nad83-epsg4269-to-wgs84-epsg4326-using-pyproj
            # https://help.arcgis.com/en/arcgisdesktop/10.0/help/index.html#/Datums/003r00000008000000/
            # https://help.arcgis.com/en/arcgisdesktop/10.0/help/index.html#/North_American_datums/003r00000009000000/

            cursor.execute(f"""
                UPDATE {DB_CONFIG['ROLL_TABLE_NAME']} 
                SET
                    lat = %s,
                    lng = %s
                    WHERE id = %s
            """, (lat, lng, id)
            )

            if i % 10_000 == 0:
                print(f'\tAt value {i}')
                conn.commit()

        conn.commit()


def create_lat_lng_columns_if_not_exists():
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor()
    cursor.execute(f"""
        ALTER TABLE {DB_CONFIG['ROLL_TABLE_NAME']}
        ADD COLUMN IF NOT EXISTS lat NUMERIC(20, 10),
        ADD COLUMN IF NOT EXISTS lng NUMERIC(20, 10);
    """)
    conn.commit()
    conn.close()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', type=Path, help="Path to the rol_unite_p.shp file")
    args = parser.parse_args()

    input_file = args.input_file

    # Parameter validation
    if not input_file.exists() or not input_file.is_file():
        print(f'Error: bad input file given')
        exit(-1)

    t0 = datetime.now()
    create_lat_lng_columns_if_not_exists()
    parse_shapefile(input_file)
    print(f'Finished in {datetime.now() - t0}')