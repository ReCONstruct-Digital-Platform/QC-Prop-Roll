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

        # Get the shapefile's CRS from the .prj file
        # Points are in NAD83 CRS while Google Maps uses WGS84
        prj_file = shp_file.with_suffix('.prj')
        with open(prj_file, 'r') as prj_file:
            nad83 = CRS.from_wkt(prj_file.read())
        wsg84 = CRS.from_epsg(4326)
        transformer = Transformer.from_crs(crs_from=nad83, crs_to=wsg84)

        for i in range(num_units):

            id = shp.record(i)[0]
            lng, lat = shp.shape(i).points[0]
            print(f'{lng, lat}')
            # Transform the coordinates, though in practice 
            # this does not seem to have an effect as NAD83 and WSG84 are very similar.
            # In QGIS, changing the CRS from one to the other performs the EPSG-1188 transformation
            # which we see here https://epsg.io/1188 is a noop.
            # https://gis.stackexchange.com/questions/304231/converting-nad83-epsg4269-to-wgs84-epsg4326-using-pyproj
            # https://help.arcgis.com/en/arcgisdesktop/10.0/help/index.html#/Datums/003r00000008000000/
            # https://help.arcgis.com/en/arcgisdesktop/10.0/help/index.html#/North_American_datums/003r00000009000000/

            t_lng, t_lat = transformer.transform(lng, lat)
            print(f'{t_lng, t_lat}')
            cursor.execute(f"""
                UPDATE {DB_CONFIG['ROLL_TABLE_NAME']} 
                SET
                    lat = %s
                    lng = %s
                    WHERE id = %s
            """, (t_lat, t_lng, id)
            )
            # Write id, lng, lat to DB




def create_lat_lng_columns_if_not_exists():
    conn = psycopg2.connect(user=DB_CONFIG['DB_USER'], password=DB_CONFIG['DB_PASSWORD'], database=DB_CONFIG['DB_NAME'])
    cursor = conn.cursor()
    cursor.execute(f"""
        ALTER TABLE {DB_CONFIG['ROLL_TABLE_NAME']}
        ADD COLUMN IF NOT EXISTS lat NUMERIC(20, 10),
        ADD COLUMN IF NOT EXISTS lng NUMERIC(20, 10),
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