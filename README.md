This project creates a connection between a mysql oltp database and postgresql olap database in a data warehouse.
The program performs a check on the last inserted row in the olap db, then dumps into it all new rows from the oltp db to update it with the transactional data obtained from daily sales.
