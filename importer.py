import pymysql.cursors
import csv
import time



class Importer():
	def __init__(self):
		# create database connection
		self.connection = pymysql.connect(host='localhost',
		                             user='root',
		                             password='asdfjkl;',
		                             db='trafficDB',
		                             cursorclass=pymysql.cursors.DictCursor)

	def boolFormat(self, str):
		return 'TRUE' if (str.upper() == 'YES') else 'FALSE'


	def floatFormat(self, str):
		try:
			return float(str)
		except:
			return -999.999999


	def createTables(self):
		# create database connection
		# connection = pymysql.connect(host='localhost',
		#                              user='root',
		#                              password='asdfjkl;',
		#                              db='trafficDB',
		#                              cursorclass=pymysql.cursors.DictCursor)

		with self.connection.cursor() as cursor:
			violationTableStmt = vehicleTableStmt = driverTableStmt = ""

			# Gets createTable sql from respective files
			with open('createTables/createTables.sql', 'r') as file:
				createTableStmt = file.read();
			
			# Read execution response from server
			cursor.execute(createTableStmt)
			print "Tables created!"

			# Commit changes 
			self.connection.commit()
 

	def populateTables(self):
		with open('TV_FORMATTED.csv', 'r') as csvfile:
			reader = csv.reader(csvfile)

			inserted_count = 0
			fail_count = 0

			for index, row in enumerate(reader, start=0):
				# Skip header row
				if index == 0:
					continue

				if index % 1000 == 0:
					print str(index), "indexes inserted", "\t Failed: ", fail_count
				# print str(index), "indexes inserted"

				current_record = reader.next()

				if len(current_record) < 35:
					# print 'bad record'
					fail_count += 1
					continue

				# Get values from reader.next()
				date, time, agency, subagency, description, location, latitude, longitude, \
				accident, belts,personal_injury, property_damage, fatal, commercial_license, \
				hazmat, commercial_vehicle, alcohol,work_zone, violation_state, vehicle_type, year, \
				make, model, color, violation_type, charge, article, contributed_to_accident,\
				race, gender, driver_city, driver_state, dl_state, arrest_type, geolocation = reader.next()

				date_time = ""
				try: date_time = str(date.split('/')[2]) + '-' + str(date.split('/')[0]) + '-' + str(date.split('/')[1]) + ' ' + time
				except:
					# print index, " date could not be configured"
					fail_count += 1
					continue


				with self.connection.cursor() as cursor:
					# NOTE: order matters here; the VIOLATION table is the child table of both DRIVER and VEHICLE tables
					# insert into VEHICLE table
					updateStmt = "INSERT INTO vehicle(vehicle_type, year, make, model, color) VALUES ('{0}',{1},'{2}','{3}','{4}')".format(vehicle_type, year, make, model, color)
					try:
						cursor.execute(updateStmt)
					except:
						# print "bad record"
						fail_count += 1
						continue
					# self.connection.commit()

					# insert into DRIVER table
					driverStmt = "INSERT INTO driver(race, gender, driver_city, driver_state, dl_state) VALUES ('{0}','{1}','{2}','{3}','{4}')".format(race, gender, driver_city, driver_state, dl_state)
					try:
						cursor.execute(driverStmt)
					except:
						# print "bad record"
						fail_count += 1
						continue
					# self.connection.commit()

					# get vehicle_id and driver_id
					getVehicle = "SELECT id FROM vehicle where vehicle_type='{0}' and year={1} and make='{2}' and model='{3}' and color='{4}'".format(vehicle_type, year, make, model, color)
					cursor.execute(getVehicle)
					vehicle_id = cursor.fetchone()['id']
					
					getDriver = "SELECT id FROM driver where race='{0}' and gender='{1}'	 and driver_city='{2}' and driver_state='{3}' and dl_state='{4}'".format(race, gender, driver_city, driver_state, dl_state)
					cursor.execute(getDriver)
					driver_id = cursor.fetchone()['id']


					# insert into VIOLATION table
					violationStmt = "INSERT INTO violation(date_time, description, location, latitude, longitude, accident, belts, personal_injury, "\
																					 "property_damage, fatal, commercial_license, hazmat, commercial_vehicle, alcohol, work_zone, "\
																					 "violation_state, violation_type, charge, article, contributed_to_accident, arrest_type, "\
																					 "agency, subagency, vehicle_id, driver_id) "\
												"VALUES ('{0}','{1}','{2}','{3}','{4}',{5},{6},{7},{8},{9},{10},{11},{12},"\
												"{13},{14},'{15}','{16}','{17}','{18}',{19},'{20}','{21}','{22}','{23}','{24}')".format(date_time, 
																																																								(description if description != None else ""),
																																																								(location if location != None else ""),
																																																								self.floatFormat(latitude),
																																																								self.floatFormat(longitude),
																																																								self.boolFormat(accident),
																																																								self.boolFormat(belts),
																																																								self.boolFormat(personal_injury),
																																																								self.boolFormat(property_damage),
																																																								self.boolFormat(fatal),
																																																								self.boolFormat(commercial_license),
																																																								self.boolFormat(hazmat),
																																																								self.boolFormat(commercial_vehicle),
																																																								self.boolFormat(alcohol),
																																																								self.boolFormat(work_zone),
																																																								(violation_state if violation_state != None else ""),
																																																								(violation_type if violation_type != None else ""),
																																																								(charge if charge != None else ""),
																																																								(article if article != None else ""),
																																																								self.boolFormat(contributed_to_accident),
																																																								(arrest_type if arrest_type != None else ""),
																																																								(agency if agency != None else ""),
																																																								(subagency if subagency != None else ""),
																																																								vehicle_id,
																																																								driver_id
																																																								)
					try:
						cursor.execute(violationStmt)
						self.connection.commit()
						inserted_count = index+1

					except:
						# print "exception!!"
						fail_count += 1
						continue

		print (inserted_count - fail_count), "records inserted into trafficDB! DONE."
		print fail_count, "records failed."



start = time.time()

i = Importer()

i.createTables()

i.populateTables()

end = time.time()

print "Total Time:", (end - start)/60.0, "minutes"

