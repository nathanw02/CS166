COPY Users
FROM '/home/csmajs/nwong063/CS166/data/users.csv'
WITH DELIMITER ',' CSV HEADER;
ALTER SEQUENCE users_userID_seq RESTART 101;

COPY Store
FROM '/home/csmajs/nwong063/CS166/data/stores.csv'
WITH DELIMITER ',' CSV HEADER;

COPY Product
FROM '/home/csmajs/nwong063/CS166/data/products.csv'
WITH DELIMITER ',' CSV HEADER;

COPY Warehouse
FROM '/home/csmajs/nwong063/CS166/data/warehouse.csv'
WITH DELIMITER ',' CSV HEADER;

COPY Orders
FROM '/home/csmajs/nwong063/CS166/data/orders.csv'
WITH DELIMITER ',' CSV HEADER;
ALTER SEQUENCE orders_orderNumber_seq RESTART 501;


COPY ProductSupplyRequests
FROM '/home/csmajs/nwong063/CS166/data/productSupplyRequests.csv'
WITH DELIMITER ',' CSV HEADER;
ALTER SEQUENCE productsupplyrequests_requestNumber_seq RESTART 11;

COPY ProductUpdates
FROM '/home/csmajs/nwong063/CS166/data/productUpdates.csv'
WITH DELIMITER ',' CSV HEADER;
ALTER SEQUENCE productupdates_updateNumber_seq RESTART 51;
