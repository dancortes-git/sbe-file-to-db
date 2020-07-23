/* Schema for postgresql */
CREATE TABLE customer_insert (
	id serial NOT NULL,
	email varchar(255) NOT NULL,
	firstName varchar(255) default NULL,
	lastName varchar(255) default NULL,
	PRIMARY KEY(id)
);