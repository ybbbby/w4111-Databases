from HW1_Template.src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        delimiter_n=self._data["connect_info"].get("delimiter")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            if delimiter_n is None:
                csv_d_rdr = csv.DictReader(txt_file)
                df = pd.read_csv(full_name)
            else:
                csv_d_rdr = csv.DictReader(txt_file,delimiter=delimiter_n)
                df = pd.read_csv(full_name, delimiter=delimiter_n)
            dup = df.duplicated(self._data["key_columns"])
            for k in dup:
                if k != False:
                    raise Exception('key is not unique')
            for r in csv_d_rdr:
                self._add_row(r)
        txt_file.close()
        self._key=[]
        for k in self._rows[0]:
            self._key.append(k)
        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)
        with open(full_name, "w") as txt_file:
            if self._data["connect_info"].get("delimiter") is None:
                csv_d_wr = csv.DictWriter(txt_file,fieldnames=self._key)
            else:
                csv_d_wr = csv.DictWriter(txt_file,fieldnames=self._key,delimiter=self._data["connect_info"].get("delimiter"))
            csv_d_wr.writeheader()
            csv_d_wr.writerows(self._rows)
        txt_file.close()

    def key_to_template(self, key):
        tmp = {}
        for k in range(len(self._data['key_columns'])):
            tmp[self._data['key_columns'][k]]= key[k]
        return tmp

    @staticmethod
    def matches_template(row, template):
        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        tem = self.key_to_template(key_fields)
        result = self.find_by_template(tem)
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
        result = []
        for r in self._rows:
            if self.matches_template(r, template):
                result.append(r)
        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        tem = self.key_to_template(key_fields)
        result = self.delete_by_template(tem)
        return result

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        result = self.find_by_template(template)
        for r in result:
            self._rows.remove(r)
        self.save()
        return len(result)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        tem = self.key_to_template(key_fields)
        result = self.update_by_template(tem, new_values)
        return result

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        result = self.find_by_template(template)
        Nsave = []
        Ndelete=[]
        for r in result:
            self._rows.remove(r)
            Nsave.append(r)
            for k, v in new_values:
                r[k] = v
            self._rows.append(r)
            Ndelete.append(r)
        self.save()
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        delimiter_n = self._data["connect_info"].get("delimiter")
        full_name = os.path.join(dir_info, file_n)
        with open(full_name, "r") as txt_file:
            if delimiter_n is None:
                df = pd.read_csv(full_name)
            else:
                df = pd.read_csv(full_name, delimiter=delimiter_n)
            dup = df.duplicated(self._data["key_columns"])
            for k in dup:
                if k != False:
                    for d in Ndelete:
                        self._rows.remove(d)
                    for s in Nsave:
                        self._rows.append(s)
                    self.save()
                    raise Exception('key is not unique')
        return len(result)

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        self._rows.append(new_record)
        self.save()
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        delimiter_n = self._data["connect_info"].get("delimiter")
        full_name = os.path.join(dir_info, file_n)
        with open(full_name, "r") as txt_file:
            if delimiter_n is None:
                df = pd.read_csv(full_name)
            else:
                df = pd.read_csv(full_name, delimiter=delimiter_n)
            dup = df.duplicated(self._data["key_columns"])
            for k in dup:
                if k != False:
                    self._rows.remove(new_record)
                    self.save()
                    raise Exception('key is not unique')

    def get_rows(self):
        return self._rows
