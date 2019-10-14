
from HW1_Template.src.CSVDataTable import CSVDataTable
import logging
import os
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")
def test_init():
    connect_info={
        "directory":data_dir,
        "file_name":"People.csv"
    }
    csv_tbl=CSVDataTable("people",connect_info,None)
    print("loaded table = \n",csv_tbl)

test_init()
def test_match():
    row={"cool":"yes","db":"no"}
    t={"cool":"yes"}
    result=CSVDataTable.matches_template(row,t)
    print(result)

test_match()

def test_match_all():
    tem={"nameLast":"Williams","birthCity": "San Diego" }
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, None)
    result=csv_tbl.find_by_template(tem)
    #result=CSVDataTable.matches_template(row,t);
    print(json.dumps(result, indent=2))

test_match_all()

def test_KtT():
    tem=["Williams"]
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    result=csv_tbl.key_to_template(tem)
    #result=CSVDataTable.matches_template(row,t);
    print(json.dumps(result, indent=2))
test_KtT()