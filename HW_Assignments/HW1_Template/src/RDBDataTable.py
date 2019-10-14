import json

from HW1_Template.src.BaseDataTable import BaseDataTable
import pymysql

class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }
        self._load()


    def _load(self):
        _host = self._data["connect_info"].get("host")
        _user = self._data["connect_info"].get("user")
        _password = self._data["connect_info"].get("password")
        _db = self._data["connect_info"].get("db")
        self.default_cnx = pymysql.connect(host='localhost',
                                           user='root',
                                           password='Summer657703',
                                           db='w4111',
                                           charset='utf8mb4',
                                           cursorclass=pymysql.cursors.DictCursor)

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        q = "select"
        if field_list is not None:
            for v in field_list:
                q = q + " " + v
        else:
            q = "select *"
        q = q + " from " + self._data["table_name"]
        q = q + " where "
        for k in range(len(key_fields)):
            q = q + self._data['key_columns'][k] + "=\"" + key_fields[k] + "\" and "
        q = q[:len(q) - 4]
        print("q=", q)
        cnx = self.default_cnx
        cursor = cnx.cursor()
        cursor.execute(q)
        result = cursor.fetchall()
        # print("Result=", json.dumps(result))
        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        q = "select"
        if field_list is not None:
            for v in field_list:
                q = q+" "+v
        else:
            q = "select *"
        q = q + " from "+self._data["table_name"]
        if template is not None:
            q = q + " where "
            for k in template:
                q = q + k + "=\"" + template[k] + "\" and "
            q = q[:len(q)-4]
        print("q=", q)
        cnx = self.default_cnx
        cursor = cnx.cursor()
        cursor.execute(q)
        result = cursor.fetchall()
        # print("Result=", json.dumps(result))
        return result


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        q = "delete from "+self._data["table_name"]+" where "
        for k in range(len(key_fields)):
            q = q + self._data['key_columns'][k] + "=\"" + key_fields[k] + "\" and "
        q = q[:len(q) - 4]

        print("q=", q)
        cnx = self.default_cnx
        cursor = cnx.cursor()
        cursor.execute(q)
        result = cursor.rowcount
        cnx.commit()
        return result

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        q = "delete from " + self._data["table_name"]+" where "
        for k in template:
            q = q + k + "=\"" + template[k] + "\" and "
        q = q[:len(q) - 4]
        print("q=", q)
        cnx = self.default_cnx
        cursor = cnx.cursor()
        cursor.execute(q)

        result = cursor.rowcount
        cnx.commit()
        # print("Result=", json.dumps(result))
        return result

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        q = "update " + self._data["table_name"] + " set "
        for k in new_values:
            q = q + k+" = \""+new_values[k]+"\", "
        q = q[:len(q) - 2]
        q = q + " where "
        for k in range(len(key_fields)):
            q = q + self._data['key_columns'][k] + "=\"" + key_fields[k] + "\" and "
        q = q[:len(q) - 4]

        print("q=", q)
        cnx = self.default_cnx
        cursor = cnx.cursor()
        cursor.execute(q)
        result = cursor.rowcount
        cnx.commit()
        return result

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        q = "update " + self._data["table_name"] + " set "
        for k in new_values:
            q = q + k + " = \"" + new_values[k] + "\", "
        q = q[:len(q) - 2]
        q = q + " where "
        for k in template:
            q = q + k + "=\"" + template[k] + "\" and "
        q = q[:len(q) - 4]
        print("q=", q)
        cnx = self.default_cnx
        cursor = cnx.cursor()
        cursor.execute(q)

        result = cursor.rowcount
        cnx.commit()
        # print("Result=", json.dumps(result))
        return result

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        q = "insert into "+self._data["table_name"] +" ("
        for k in new_record:
            q = q+k+", "
        q = q[:len(q) - 2]
        q = q + ") values ("
        for k in new_record:
            q = q+"\""+new_record[k]+"\", "
        q = q[:len(q) - 2]
        q = q+")"
        print("q=", q)
        cnx = self.default_cnx
        cursor = cnx.cursor()
        cursor.execute(q)

        result = cursor.rowcount
        cnx.commit()

    def get_rows(self):
        return self.find_by_template(None)




