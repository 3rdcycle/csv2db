# -*- encoding: utf-8 -*-
"""
Sample import script to demonstrate the use of csv2db.py

The script generates an 'import.sql' file that inserts departments and 
employees into a database consisting of four tables:

- publicobject: A metadata table that contains a record for each 'real' object 
                with the insertion date of that object
- departments:  List of departments
- employees:    List of employees, each belonging to exactly one department
- phones:       List of phones, zero or one per employee

The primary key for all records is '_oid'. 

"""
import sys
import os
sys.path.append(os.path.abspath('..'))
from csv2db import CsvImporter, RecordSpec, ColumnValue, MultiColumnValue, \
                   ConstValue, DynamicValue, XReference


# We store the department records for later when we import employees
# and need to link them to their respective departments
departments = []


def main():
    # See python csv module for dialect specifications
    # Attention: skipinitialspace does not skip e.g. tabs. Make sure your
    # csv contains spaces only
    dialect = {
        'delimiter': ',',
        'skipinitialspace': True
    }

    sql = ''

    # Import departments first
    importer = CsvImporter('departments.txt', dialect, DEP_IMPORT_SPEC)
    # Each DbRecord has a row_id attribute which is filled from the 
    # 'Department' column. We use this to link employees to departments
    # later.
    db_records = importer.import_data(id_col='Department')
    # Generate insert statements for each DbRecord object
    for record in db_records:
        sql += record.insert_statement()
        # store department records for later
        if record.table_name == 'departments':
            departments.append(record)
    # Import employees
    importer = CsvImporter('employees.txt', dialect, EMP_IMPORT_SPEC)
    db_records = importer.import_data()
    for record in db_records:
        sql += record.insert_statement()

    # Write combined SQL to file
    with open('import.sql', 'w+') as f:
        f.write(sql)


# Helper functions used in import specifications
# ----------------------------------------------

class OidFactory:
    ''' Creates sequential _oid numbers '''
    def __init__(self):
        self.oid = 0
    def __call__(self, row):
        self.oid += 1
        return str(self.oid)

def quote(value):
    ''' Char type columns require extra quotes '''
    return "'{}'".format(value)

def make_name(values):
    ''' Concatenates first and last name '''
    return "'{} {}'".format(values['First'], values['Last'])

def department_oid(name):
    ''' Looks up department _oid from the department records created earlier'''
    return next(d.attributes['_oid'] for d in departments if d.row_id == name)

def has_phone(row):
    ''' Checks if a particular row contains a phone number '''
    return True if row['Phone'] != '-' else False


# Import specifications
# ---------------------

# Common

PUBLIC_OBJECT_MAP = {
    '_oid':             DynamicValue(OidFactory()),                             # Compute _oid dynamically
    'date':             ConstValue('now()')                                     # Insertion date, now() is a DB function
}

# Department import specification   

DEPARTMENT_MAP = {
    '_oid':             DynamicValue(OidFactory()),                             # Compute _oid dynamically
    '_publicobj_oid':   XReference('publicobject', 'sole', '_oid'),             # Take _oid value from 'sole' instance in 'publicobject' table
    'name':             ColumnValue('Department', convert=quote),               # Read from csv and put into quotes
    'floor':            ColumnValue('Floor', convert=quote)                     # Read from csv and put into quotes
}

DEP_IMPORT_SPEC = {                                                             # For each imported row create... 
    'publicobject': {
        'sole':         RecordSpec(attr_map=PUBLIC_OBJECT_MAP)                  # ... one record in the 'publicobject' table
    },
    'departments': {
        'sole':         RecordSpec(attr_map=DEPARTMENT_MAP)                     # ... one record in the 'departments' table
    }
}

# Employee import specification 

EMPLOYEE_MAP = {
    '_oid':             DynamicValue(OidFactory()),                             # Compute _oid dynamically
    '_publicobj_oid':   XReference('publicobject', 'emp', '_oid'),              # Take _oid value from 'emp' instance in 'publicobject' table
    'department_oid':   ColumnValue('Department', convert=department_oid),      # Lookup department _oid by name
    'name':             MultiColumnValue(['First', 'Last'], convert=make_name)  # name = First + Last
}

PHONE_MAP = {
    '_oid':             DynamicValue(OidFactory()),                             # Compute _oid dynamically
    '_publicobj_oid':   XReference('publicobject', 'phone', '_oid'),            # Take _oid value from 'phone' instance in 'publicobject' table
    '_employee_oid':    XReference('employees', 'sole', '_oid'),                # Take _oid value from 'sole' instance in 'employees' table
    'number':           ColumnValue('Phone', convert=quote)                     # Read from csv and put into quotes
}
                            
EMP_IMPORT_SPEC = {                                                             # For each imported row create...
    'publicobject': {
        'emp':          RecordSpec(attr_map=PUBLIC_OBJECT_MAP),                 # ... one record in the 'publicobject' table for the employee
        'phone':        RecordSpec(attr_map=PUBLIC_OBJECT_MAP,                  # ... another one for the phone record we're going to create later
                                   condition=has_phone),                        # but only if the row contains a phone number
    },
    'employees': {
        'sole':         RecordSpec(attr_map=EMPLOYEE_MAP)                       # ... one record in the 'employees' table
    },
    'phones': {
        'sole':         RecordSpec(attr_map=PHONE_MAP, condition=has_phone)     # ... one record in the 'phones' table if the employee has a phone
    },
}


if __name__ == '__main__':
    main()