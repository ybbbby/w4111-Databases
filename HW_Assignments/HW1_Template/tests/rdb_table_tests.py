from HW1_Template.src.RDBDataTable import RDBDataTable
import pymysql
import pandas as pd
import json

table_name="people"
connect_info = {
    "host": "localhost",
    "user": "root",
    "password" : "*******",
    "db" : "w4111"
}


def test_find_by_template():
    db=RDBDataTable(table_name,connect_info,["playerId"])
    result=db.find_by_template({"playerID":"1234"})
    print(result)


test_find_by_template()


def test_find_by_key():
    db=RDBDataTable(table_name,connect_info,["playerId"])
    result=db.find_by_primary_key(["aaronto01"])
    print(json.dumps(result))


test_find_by_key()


def test_delete_by_key():
    db=RDBDataTable(table_name,connect_info,["playerId"])
    result=db.delete_by_key(["acunaro01"])
    print(json.dumps(result))


test_delete_by_key()


def test_delete_by_template():
    db=RDBDataTable(table_name,connect_info,["playerId"])
    result=db.delete_by_template({"birthDay":"11"})
    print(json.dumps(result))

test_delete_by_template()


def test_update_by_key():
    db=RDBDataTable(table_name,connect_info,["playerId"])
    result=db.update_by_key(["aaronto01"],{"birthDay":"13"})
    print(json.dumps(result))


test_update_by_key()


def test_update_by_template():
    db=RDBDataTable(table_name,connect_info,["playerId"])
    result=db.update_by_template({"playerID":"aaronto01"},{"birthDay":"16"})
    print(json.dumps(result))


test_update_by_template()


def test_insert():
    db=RDBDataTable(table_name,connect_info,["playerId"])
    db.insert({"playerID":"1234"})


test_insert()


def test_findRow():
    db=RDBDataTable(table_name,connect_info,["playerId"])
    result=db.find_by_template(None)
    print(len(result))


test_findRow()