CREATE DATABASE IF NOT EXISTS trafficDB;

DROP TABLE IF EXISTS violation;
DROP TABLE IF EXISTS vehicle;
DROP TABLE IF EXISTS driver;

CREATE TABLE IF NOT EXISTS vehicle(
	id int AUTO_INCREMENT,
	vehicle_type varchar(30),
	year int,
	make varchar(30),
	model varchar(30),
	color varchar(30),
	PRIMARY KEY (id)
) ENGINE=INNODB;


CREATE TABLE IF NOT EXISTS driver(
	id int AUTO_INCREMENT,
	race varchar(30),
	gender char(1),
	driver_city varchar(30),
	driver_state char(2),
	dl_state char(2),
	PRIMARY KEY (id)
) ENGINE=INNODB;

-- Child of both VEHICLE and DRIVER tables
CREATE TABLE IF NOT EXISTS violation(
	id int AUTO_INCREMENT,
	date_time datetime,
	description tinytext,
	location tinytext,
	latitude float(15,12),
	longitude float(15,12),
	accident boolean,
	belts boolean,
	personal_injury boolean,
	property_damage boolean,
	fatal boolean,
	commercial_license boolean,
	hazmat boolean,
	commercial_vehicle boolean,
	alcohol boolean,
	work_zone boolean,
	violation_state varchar(3),
	violation_type varchar(30),
	charge varchar(30),
	article varchar(30),
	contributed_to_accident boolean,
	arrest_type varchar(30),
	agency varchar(10),
	subagency varchar(100),
	vehicle_id int NOT NULL,
	driver_id int NOT NULL,

	PRIMARY KEY (id),
	
	FOREIGN KEY (vehicle_id)
		REFERENCES vehicle(id),

	FOREIGN KEY (driver_id)
		REFERENCES driver(id)
) ENGINE=INNODB;