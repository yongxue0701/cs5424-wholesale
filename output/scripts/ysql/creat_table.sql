CREATE TABLE IF NOT EXISTS Warehouse (                                                                                                                                                                  
W_ID integer PRIMARY KEY,
W_NAME varchar(10),
W_STREET_1 varchar(20),
W_STREET_2 varchar(20),
W_CITY varchar(20),
W_STATE char(2),
W_ZIP char(9),
W_TAX decimal(4,4),
W_YTD decimal(12,2)
);

CREATE TABLE IF NOT EXISTS District (
D_W_ID integer,
D_ID integer,
D_NAME varchar(10),
D_STREET_1 varchar(20),
D_STREET_2 varchar(20),
D_CITY varchar(20),
D_STATE char(2),
D_ZIP char(9),
D_TAX decimal(4,4),
D_YTD decimal(12,2),
D_NEXT_O_ID integer,
CONSTRAINT fk_district FOREIGN KEY (D_W_ID) REFERENCES Warehouse(W_ID),
PRIMARY KEY (D_W_ID, D_ID)
);

CREATE TABLE IF NOT EXISTS Customer (
C_W_ID integer,
C_D_ID integer,
C_ID integer,
C_FIRST varchar(16),
C_MIDDLE char(2),
C_LAST varchar(16),
C_STREET_1 varchar(20),
C_STREET_2 varchar(20),
C_CITY varchar(20),
C_STATE char(2),
C_ZIP char(9),
C_PHONE char(16),
C_SINCE timestamptz,
C_CREDIT CHAR(2),
C_CREDIT_LIM decimal(12,2),
C_DISCOUNT decimal(5,4),
C_BALANCE decimal(12,2),
C_YTD_PAYMENT float,
C_PAYMENT_CNT integer,
C_DELIVERY_CNT integer,
C_DATA varchar(500),
CONSTRAINT fk_customer FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES District(D_W_ID, D_ID),

PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
);

CREATE TABLE IF NOT EXISTS Orders (
O_W_ID integer,
O_D_ID integer,
O_ID integer,
O_C_ID integer,
O_CARRIER_ID integer CONSTRAINT range_carrier_id CHECK (O_CARRIER_ID >=1 AND O_CARRIER_ID <=10),
O_OL_CNT decimal(2,0),
O_ALL_LOCAL decimal(1,0),
O_ENTRY_D timestamptz,
CONSTRAINT fk_order FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES Customer(C_W_ID, C_D_ID, C_ID),
PRIMARY KEY (O_W_ID, O_D_ID, O_ID)
);

CREATE TABLE IF NOT EXISTS Item (
I_ID integer PRIMARY KEY,
I_NAME varchar(24),
I_PRICE decimal(5,2),
LIM_ID integer,
I_DATA varchar(50)
);

CREATE TABLE IF NOT EXISTS Order_Line (
OL_W_ID integer,
OL_D_ID integer,
OL_O_ID integer,
OL_NUMBER integer,
OL_I_ID integer,
OL_DELIVERY_D timestamptz,
OL_AMOUNT decimal(7,2),
OL_SUPPLY_W_ID integer,
OL_QUANTITY decimal(2,0),
OL_DIST_INFO char(24),
CONSTRAINT fk_order_line FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES Orders(O_W_ID, O_D_ID, O_ID),
PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
);

CREATE TABLE IF NOT EXISTS Stock (
S_W_ID integer,
S_I_ID integer,
S_QUANTITY decimal(4,0),
S_YTD decimal(8,2),
S_ORDER_CNT integer,
S_REMOTE_CNT integer,
S_DIST_01 char(24),
S_DIST_02 char(24),
S_DIST_03 char(24),
S_DIST_04 char(24),
S_DIST_05 char(24),
S_DIST_06 char(24),
S_DIST_07 char(24),
S_DIST_08 char(24),
S_DIST_09 char(24),
S_DIST_10 char(24),
S_DATA varchar(50),
CONSTRAINT fk_stock_1 FOREIGN KEY (S_I_ID) REFERENCES Item(I_ID),
CONSTRAINT fk_stock_2 FOREIGN KEY (S_W_ID) REFERENCES Warehouse(W_ID),
PRIMARY KEY (S_W_ID, S_I_ID)
);
