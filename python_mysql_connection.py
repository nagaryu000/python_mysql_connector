# coding: utf-8

import mysql.connector
from datetime import date, datetime, timedelta
from mysql.connector import errorcode


DB_NAME = 'mysql_connector_python'
TABLES = {}

conn = mysql.connector.connect(
                                user='nagaryu000',
                                password='ryujinagasawa',
                                host='127.0.0.1',
                                )

cursor = conn.cursor()
cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")


def create_database(cursor):
    try:
        cursor.execute(
            f"CREATE DATABASE {DB_NAME} DEFAULT CHARACTER SET 'utf8mb4'"
        )
    except mysql.connector.Error as err:
        print(f"Failed creating database: {err}")
        exit(1)
try:
    cursor.execute(f"USE {DB_NAME}")
except mysql.connector.Error as err:
    print(f"Database {DB_NAME} does not exists.")
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print(f"Database {DB_NAME} created successfully.")
        conn.database = DB_NAME
    else:
        print(err)
        exit(1)

# table columns
TABLES['employees'] = (
    "CREATE TABLE `employees` ("
    "`emp_no` int(11) NOT NULL AUTO_INCREMENT,"
    "`birth_date` date NOT NULL,"
    "`first_name` varchar(30) NOT NULL,"
    "`last_name` varchar(30) NOT NULL,"
    "`gender` enum('M', 'F') NOT NULL,"
    "`hire_date` date NOT NULL,"
    "PRIMARY KEY (`emp_no`)"
    ") ENGINE=InnoDB")

TABLES['departments'] = (
    "CREATE TABLE `departments` ("
    "`dept_no` char(4) NOT NULL,"
    "`dept_name` char(40) NOT NULL,"
    "PRIMARY KEY (`dept_no`), UNIQUE KEY `dept_name` (`dept_name`)"
    ") ENGINE=InnoDB")

TABLES['salaries'] = (
    "CREATE TABLE `salaries` ("
    "`emp_no` int(11) NOT NULL,"
    "`salary` int(11) NOT NULL,"
    "`from_date` date NOT NULL,"
    "`to_date` date NOT NULL,"
    "PRIMARY KEY (`emp_no`, `from_date`), KEY `emp_no` (`emp_no`), "
    "CONSTRAINT `salaries_ibfk_1` FOREIGN KEY (`emp_no`) "
    "REFERENCES `employees` (`emp_no`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['dept_emp'] = (
    "CREATE TABLE `dept_emp` ("
    "`emp_no` int(11) NOT NULL,"
    "`dept_no` char(4) NOT NULL,"
    "`from_date` date NOT NULL,"
    "`to_date` date NOT NULL,"
    "PRIMARY KEY (`emp_no`, `dept_no`), KEY `emp_no` (`emp_no`),"
    "KEY `dept_no` (`dept_no`),"
    "CONSTRAINT `dept_emp_ibfk_1` FOREIGN KEY (`emp_no`) "
    "REFERENCES `employees` (`emp_no`) ON DELETE CASCADE, "
    "CONSTRAINT `dept_emp_ibfk_2` FOREIGN KEY (`dept_no`) "
    "REFERENCES `departments` (`dept_no`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['dept_manager'] = (
    "CREATE TABLE `dept_manager` ("
    "`dept_no` char(4) NOT NULL,"
    "`emp_no` int(11) NOT NULL,"
    "`from_date` date NOT NULL,"
    "`to_date` date NOT NULL,"
    "PRIMARY KEY (`emp_no`, `dept_no`),"
    "KEY `emp_no` (`emp_no`),"
    "KEY `dept_no` (`dept_no`),"
    "CONSTRAINT `dept_manager_ibfk_1` FOREIGN KEY (`emp_no`) "
    "REFERENCES `employees` (`emp_no`) ON DELETE CASCADE,"
    "CONSTRAINT `dept_manager_ibfk_2` FOREIGN KEY (`dept_no`) "
    "REFERENCES `departments` (`dept_no`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['titles'] = (
    "CREATE TABLE `titles` ("
    "`emp_no` int(11) NOT NULL,"
    "`title` varchar(50) NOT NULL,"
    "`from_date` date NOT NULL,"
    "`to_date` date DEFAULT NULL,"
    "PRIMARY KEY (`emp_no`, `title`, `from_date`), KEY `emp_no` (`emp_no`),"
    "CONSTRAINT `titles_ibfk_1` FOREIGN KEY (`emp_no`) "
    "REFERENCES `employees` (`emp_no`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

# Create tables
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print(f"Creating table {table_name}", end=" ")
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK!")

# insert records
tomorrow = datetime.now().date() + timedelta(days=1)

add_employee = ("INSERT INTO employees "
                "(first_name, last_name, hire_date, gender, birth_date) "
                "values (%s, %s, %s, %s, %s)")

add_salary = ("INSERT INTO salaries "
                "(emp_no, salary, from_date, to_date) "
                "values (%(emp_no)s, %(salary)s, %(from_date)s, %(to_date)s)")

data_employee = ("けんたろう", "鈴木", tomorrow, 'F', date(1979, 5, 27))

# Insert a new employee
cursor.execute(add_employee, data_employee)
emp_no = cursor.lastrowid

# Insert salary information
data_salary = {
    'emp_no': emp_no,
    'salary': 500000,
    'from_date': tomorrow,
    'to_date': date(9999, 12, 31),
}
cursor.execute(add_salary, data_salary)

# Make sure data is commited to the database
conn.commit()

cursor.close()
conn.close()