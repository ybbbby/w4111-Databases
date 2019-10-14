from HW1_Template.src.CSVDataTable import CSVDataTable
import logging
import os
import time
import json


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.ERROR)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../../../Data/ClassicModels/")


def test_load_fail():

    print("\n\n")
    print("******************** " + "test_load_fail" + " ********************")
    connect_info = {
        "directory": data_dir,
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    print("Table is orderdetails and key_columns are ['orderNumber']")
    csv_tbl = CSVDataTable("orderdetails", connect_info, key_columns=['orderNumber'])

    print("Loaded table = \n", csv_tbl)
    print("This is the wrong answer")


    print("******************** " + "end test_load_fail" + " ********************")


def test_load_good():

    print("\n\n")
    print("******************** " + "test_load_good" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    csv_tbl = CSVDataTable("orderdetails", connect_info,
                           key_columns=['orderNumber', "orderLineNumber"])

    print("Loaded table = \n", csv_tbl)
    print("This is the correct answe")



    print("******************** " + "end test_load_ggod" + " ********************")


#test_load_fail()
test_load_good()
def test_load_fail2():

    print("\n\n")
    print("******************** " + "test_load_fail2" + " ********************")
    connect_info = {
        "directory": data_dir,
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    print("Table is orderdetails and key_columns are ['orderNumber', 'cat']")
    csv_tbl = CSVDataTable("orderdetails", connect_info, key_columns=['orderNumber', 'cat'])

    print("Loaded table = \n", csv_tbl)
    print("This is the wrong answer")


    print("******************** " + "end test_load_fail" + " ********************")
# test_load_fail2()
def test_delete_good():

    print("\n\n")
    print("******************** " + "test_delete_good" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    csv_tbl = CSVDataTable("orderdetails", connect_info,
                           key_columns=['orderNumber', "orderLineNumber"])

    r1 = csv_tbl.find_by_template({"orderNumber": "10100"})
    print("Details for order '10100' = \n", json.dumps(r1, indent=2))

    print("\nDeleting productCode 'S18_1749':")
    r2 = csv_tbl.delete_by_template({"orderNumber": "10100", "productCode": 'S18_1749'})

    print("Delete returned ", r2, "\n")

    r3 = csv_tbl.find_by_template({"orderNumber": "10100"})
    print("Details for order '10100' after delete = \n", json.dumps(r3, indent=2))


    #print("Loaded table = \n", csv_tbl)
    print("This is the correct answer")



    print("******************** " + "end test_ldelete_good" + " ********************")
test_delete_good()
def test_insert_bad():

    print("\n\n")
    print("******************** " + "test_insert_bad" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }


    bad_row = {
         "orderNumber": "10100",
         "productCode": "S24_3969",
         "quantityOrdered": "49",
         "priceEach": "35.29",
         "orderLineNumber": "1"
         }

    csv_tbl = CSVDataTable("orderdetails", connect_info,
                           key_columns=['orderNumber', "orderLineNumber"])

    key = ['10100', "1"]
    print("Looking up with key = ", key)
    row = csv_tbl.find_by_primary_key(key)
    print("Returned row = ", json.dumps(bad_row, indent=2))

    print("\nAttempting to insert bad row = ", json.dumps(bad_row, indent=2))
    res = csv_tbl.insert(bad_row)
    print("Result = ", res, "This is an error!")



    print("******************** " + "end test_insert_bad" + " ********************")
test_insert_bad()