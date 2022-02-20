CREATE TABLE h_orders ( 
	h_order_rk SERIAL PRIMARY KEY, 
	orderid int, 
	source_system varchar, 
	processed_dttm date NULL 
	);

CREATE TABLE s_orders ( 
	h_order_rk int, 
	orderdate date NULL,
	orderstatus varchar(1) NULL,
	orderpriority varchar(15) NULL,
	clerk varchar(15) NULL,
	source_system varchar, 
	valid_from_dttm date NULL, 
	valid_to_dttm date NULL, 
	processed_dttm date NULL,
	CONSTRAINT s_h_orders_fk FOREIGN KEY (h_order_rk) REFERENCES h_orders(h_order_rk)
	);

CREATE TABLE h_products ( 
	h_product_rk SERIAL PRIMARY KEY, 
	productid int, 
	source_system varchar, 
	processed_dttm date NULL 
	);

CREATE TABLE s_products ( 
	h_product_rk int, 
	productname varchar(55) NULL,
	producttype varchar(25) NULL,
	productsize int4 NULL,
	retailprice numeric(15,2) NULL,
	source_system varchar, 
	valid_from_dttm date NULL, 
	valid_to_dttm date NULL, 
	processed_dttm date NULL,
	CONSTRAINT s_h_products_fk FOREIGN KEY (h_product_rk) REFERENCES h_products(h_product_rk)
	);

CREATE TABLE h_suppliers ( 
	h_supplier_rk SERIAL PRIMARY KEY, 
	supplierid int, 
	source_system varchar, 
	processed_dttm date NULL 
	);

CREATE TABLE s_suppliers ( 
	h_supplier_rk int, 
	suppliername varchar(25) NULL,
	address varchar(40) NULL,
	phone varchar(15) NULL,
	balance numeric(15,2) NULL,
	descr varchar(101) NULL,
	source_system varchar, 
	valid_from_dttm date NULL, 
	valid_to_dttm date NULL, 
	processed_dttm date NULL,
	CONSTRAINT s_h_suppliers_fk FOREIGN KEY (h_supplier_rk) REFERENCES h_suppliers(h_supplier_rk)
	);

CREATE TABLE l_orderdetails ( 
	l_orderdetails_rk SERIAL PRIMARY KEY, 
	h_order_rk int,
	h_product_rk int, 
	source_system varchar, 
	processed_dttm date NULL,
	CONSTRAINT l_orderdetails_fk1 FOREIGN KEY (h_order_rk) REFERENCES h_orders(h_order_rk),
	CONSTRAINT l_orderdetails_fk2 FOREIGN KEY (h_product_rk) REFERENCES h_products(h_product_rk)
	);

CREATE TABLE s_orderdetails ( 
	l_orderdetails_rk int, 
	unitprice numeric(15,2) NULL,
	quantity numeric(15,2) NULL,
	discount numeric(15,2) NULL,
	source_system varchar, 
	valid_from_dttm date NULL, 
	valid_to_dttm date NULL, 
	processed_dttm date NULL,
	CONSTRAINT s_l_orderdetails_fk FOREIGN KEY (l_orderdetails_rk) REFERENCES l_orderdetails(l_orderdetails_rk)
	);

CREATE TABLE l_productsuppl ( 
	l_productsuppl_rk SERIAL PRIMARY KEY, 
	h_product_rk int, 
	h_supplier_rk int, 
	source_system varchar, 
	processed_dttm date NULL,
	CONSTRAINT l_productsuppl_fk1 FOREIGN KEY (h_supplier_rk) REFERENCES h_suppliers(h_supplier_rk),
	CONSTRAINT l_productsuppl_fk2 FOREIGN KEY (h_product_rk) REFERENCES h_products(h_product_rk)
	);

CREATE TABLE s_productsuppl ( 
	l_productsuppl_rk int, 
	qty int4 NULL,
	supplycost numeric(15,2) NULL,
	descr varchar(199) NULL,
	source_system varchar, 
	valid_from_dttm date NULL, 
	valid_to_dttm date NULL, 
	processed_dttm date NULL,
	CONSTRAINT s_l_productsuppl_fk FOREIGN KEY (l_productsuppl_rk) REFERENCES l_productsuppl(l_productsuppl_rk)
	);
