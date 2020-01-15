import sys, os, tempfile, json, logging, arcpy, fnmatch, shutil, subprocess, arcgis, getpass
from arcgis.gis import GIS
import datetime as dt
from urllib import request
from urllib.error import URLError
import pandas as pd

def calculate_datetime(text_field):
    date_string = str(text_field)
    if '.' in date_string:
        utc_now = dt.datetime.utcnow()
        month = utc_now.month
        year = utc_now.year
        day = date_string.split('.')[0]
        hour = date_string.split('.')[1]
        str_date = str(month) + '/' + day + '/' + str(year) + ' ' + hour + ':00'
        new_date = datetime.datetime.strptime(str_date, '%m/%d/%Y %H:%M')
        return new_date
    else:
        pass

def feedRoutine(workGDB):
    # Log file
    logging.basicConfig(filename="cbrfc_river_conditions_update.log", level=logging.INFO)
    log_format = "%Y-%m-%d %H:%M:%S"
    # Create workGDB and default workspace
    print("Starting workGDB...")
    logging.info("Starting workGDB... {0}".format(dt.datetime.now().strftime(log_format)))
    arcpy.env.workspace = workGDB
    if arcpy.Exists(arcpy.env.workspace):
        for feat in arcpy.ListTables("CBRFC_*"):
            arcpy.management.Delete(feat)
    else:
        arcpy.management.CreateFileGDB(os.path.dirname(workGDB), os.path.basename(workGDB))

    # Download the latest stream gauge conditions and load into a pandas dataframe
    print("Downloading data...")
    logging.info("Downloading data... {0}".format(dt.datetime.now().strftime(log_format)))
    temp_dir = tempfile.mkdtemp()
    # filename = os.path.join(temp_dir, 'latest_data.json')
    try:
        url = 'https://www.cbrfc.noaa.gov/gmap/list/list.php?search=&point=forecast&plot=&sort=riverhis&psv=on&type=river&basin=0&subbasin=&espqpf=0&espdist=emperical'
    except URLError:
        logging.exception("Failed on: request.urlretrieve(url, filename) {0}".format(dt.datetime.now().strftime(log_format)))
        raise Exception("{0} not available. Check internet connection or url address".format(url))
    
    df = pd.read_csv(url)
    column_list = ['NWS_ID', 'River', 'Location', 'Forecast_Condition', 'Point_Type', 'Observed_DayTime', 'Latest_Flow', 'Latest_Stage', 'Flood_Stage', 'Bankfull_Stage', 'HUC', 'State', 'HSA', 'Elevation', 'Forecast_Group', 'Segment', 'DeleteMe', 'DeleteMe_2']
    df.columns = column_list
    df = df.drop(['DeleteMe', 'DeleteMe_2'], axis=1)
    print(df.head())

    # Export pandas dataframe to temp CSV
    out_csv_path = os.path.join(temp_dir, 'work.csv')
    df.to_csv(out_csv_path)

    # Convert temp CSV into temp GDB table
    arcpy.conversion.TableToTable(out_csv_path, workGDB, 'CBRFC_data_table')
    temp_gdb_table = os.path.join(workGDB, 'CBRFC_data_table')
    arcpy.management.AddField(temp_gdb_table, 'Obs_DateTime', 'DATE', field_alias='Observation Day and Time')

    # Convert day and time observations into a datetime field
    fields = ['Observed_DayTime', 'Obs_DateTime']
    with arcpy.da.UpdateCursor(temp_gdb_table, fields) as cursor:
        for row in cursor:
            if row[0] is not None:
                row[1] = calculate_datetime(row[0])
            cursor.updateRow(row)

    # Search cursor on temp GDB table, update cursor on work GDB stream gauge conditions layer


    # Convert json files to features
    print("Creating feature classes...")
    logging.info("Creating feature classes... {0}".format(dt.datetime.now().strftime(log_format)))
    

    # Deployment Logic
#     print("Deploying...")
#     logging.info("Deploying... {0}".format(dt.datetime.now().strftime(log_format)))
#     deployLogic(workGDB, itemid, original_sd_file, service_name)

    # Close log file
    logging.shutdown()

    # Return
    print("Done!")
    logging.info("Done! {0}".format(dt.datetime.now().strftime(log_format)))
    return True

def deployLogic(workGDB, itemid, original_sd_file, service_name):
    gis = GIS(url='https://arcgis.com', username=username, password=password)
    item = gis.content.get(itemid)
    sd_file_name = os.path.basename(original_sd_file)
    if sd_file_name != item.related_items("Service2Data")[0].name:
        raise Exception('Erroneous itemid, service name or original sd file'.format(itemid))
    # Unpack original_sd_file using 7-zip
    path_7z = fnmatch.filter(os.environ['path'].split(';'), '*7-Zip')
    temp_dir = tempfile.mkdtemp()
    if len(path_7z):
        exe_7z = os.path.join(path_7z[0], '7z.exe')
        call_unzip = '{0} x {1} -o{2}'.format(exe_7z, original_sd_file, temp_dir)
    else:
        raise Exception('7-Zip could not be found in the PATH environment variable')
    subprocess.call(call_unzip)
    # Replace Live.gdb content
    liveGDB = os.path.join(temp_dir, 'p20', 'live.gdb')
    # os.mkdir(os.path.join(temp_dir, 'p20'))
    arcpy.management.CreateFileGDB(os.path.dirname(liveGDB), os.path.basename(liveGDB))
    shutil.rmtree(liveGDB)
    os.mkdir(liveGDB)
    for root, dirs, files in os.walk(workGDB):
        files = [f for f in files if '.lock' not in f]
        for f in files:
            shutil.copy2(os.path.join(workGDB, f), os.path.join(liveGDB, f))
    # Zip file
    os.chdir(temp_dir)
    updated_sd = os.path.join(temp_dir, sd_file_name)
    call_zip = '{0} a {1} -m1=LZMA'.format(exe_7z, updated_sd)
    subprocess.call(call_unzip)
    # Replace file
    manager = arcgis.features.FeatureLayerCollection.fromitem(item).manager
    status = manager.overwrite(updated_sd)
    # Return
    return True

if __name__ == "__main__":
    [url, workGDB, itemid, original_sd_file, service_name] = sys.argv[1:]
    feedRoutine(url, workGDB, itemid, original_sd_file, service_name)