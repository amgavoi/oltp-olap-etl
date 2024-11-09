'''This automated program connects a mysql oltp database with a postgresql olap database
in a data warehouse to update the latter with the transactional data obtained from daily sales'''

# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='s6ZNZ2DyQ23FVzUaol2GJo3m',host='172.21.34.161',database='sales')
if (connection):
	print("Connected to MySQL")
cursor = connection.cursor()

# Connect to DB2 or PostgreSql
dsn_hostname = '172.21.188.30'
dsn_user='postgres'
dsn_pwd ='lsfpCjUTuNFq9MJBsAxFvwBd'
dsn_port ="5432"
dsn_database ="sales"
conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port)
if (conn):
    print("Connected to PostgreSql")
curs = conn.cursor()

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.
def get_last_rowid():
    sql = '''select max(rowid) from sales_data;'''
    curs.execute(sql)
    row = curs.fetchone()
    rowid = row[0]
    return rowid

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.
def get_latest_records(rowid):
    sql = '''select * from sales_data where rowid >'''+str(rowid)
    cursor.execute(sql)
    records = cursor.fetchall()
    return records

new_records = get_latest_records(last_row_id)
print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.
def insert_records(records):
    if len(records) > 0:
        sql = '''insert into sales_data(rowid, productid, customerid, quantity) values(%s, %s, %s, %s);'''
        for row in records:
            curs.execute(sql, row)
            conn.commit()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql database
cursor.close()
connection.close()
print('Disconnected from MySQL')

# disconnect from DB2 or PostgreSql data warehouse 
curs.close()
conn.close()
print('Disconnected from PostgreSql')
# End of program