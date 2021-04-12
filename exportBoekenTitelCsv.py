#!/usr/bin/env python3

"""exportBoekenTitelCsv.py: Export table titel of database boeken as CSV"""

__author__ = "Chris van Engelen"
__copyright__ = "Copyright 2021"

import sys
import os.path
import mysql.connector
from mysql.connector import errorcode
import datetime
import argparse
import configparser

# Process command line arguments
parser = argparse.ArgumentParser()
defaultConfigPath = "database.ini"
parser.add_argument("-c", "--configPath", help="database configuration file path (default " + defaultConfigPath + ")",
                    default=defaultConfigPath)
parser.add_argument("-o", "--outputPath", help="CSV output file path (default none)")
args = parser.parse_args()

# Check if the database configuration file exists
if not os.path.isfile(args.configPath):
    print("Configuration file", args.configPath, "not found")
    exit(1)

# Read the database configuration file
databaseConfig = configparser.ConfigParser()
databaseConfig.read(args.configPath)
mysqlConnectorConfig = {
    'host': databaseConfig['connection']['host'],
    'user': databaseConfig['boeken']['user'],
    'password': databaseConfig['boeken']['password'],
    'database': databaseConfig['boeken']['database'],
    'raise_on_warnings': databaseConfig.getboolean('general', 'raise_on_warnings')
}

try:
    # Setup the MySQL query on table titel of database boeken
    query = ("SELECT titel.titel, auteurs.auteurs, persoon.persoon as auteur, titel.jaar, "
             "type.type, onderwerp.onderwerp, vorm.vorm, taal.taal, titel.opmerkingen, "
             "boek.boek, uitgever.uitgever, status.status, boek.datum "
             "FROM titel "
             "LEFT JOIN auteurs ON auteurs.auteurs_id = titel.auteurs_id "
             "LEFT JOIN auteurs_persoon ON auteurs_persoon.auteurs_id = titel.auteurs_id "
             "LEFT JOIN persoon ON persoon.persoon_id = auteurs_persoon.persoon_id "
             "LEFT JOIN onderwerp ON onderwerp.onderwerp_id = titel.onderwerp_id "
             "LEFT JOIN vorm ON vorm.vorm_id = titel.vorm_id "
             "LEFT JOIN taal ON taal.taal_id = titel.taal_id "
             "LEFT JOIN boek on titel.boek_id = boek.boek_id "
             "LEFT JOIN type ON type.type_id = boek.type_id "
             "LEFT JOIN uitgever ON uitgever.uitgever_id = boek.uitgever_id "
             "LEFT JOIN status ON status.status_id = boek.status_id " +
             "ORDER BY persoon.persoon, titel.titel")

    # Setup a connection to the MySQL database
    mysqlConnection = mysql.connector.connect(**mysqlConnectorConfig)

    # Execute the query
    cursor = mysqlConnection.cursor()
    cursor.execute(query)

    # Open the output file
    boekenTitelCsvFile = open(args.outputPath, mode='w', encoding='iso-8859-1') if args.outputPath else sys.stdout

    # Print the CSV header
    print("Titel,Auteurs,Auteur,Jaar,Type,Onderwerp,Vorm,Taal,Opmerkingen,Boek,Uitgever,Status,Datum",
          file=boekenTitelCsvFile)

    # Define a date object with datetime
    boekDatumDate = datetime.date(1, 1, 1)

    for (titel, titelAuteurs, titelAuteur, titelJaarInt, titelType,
         titelOnderwerp, titelVorm, titelTaal, titelOpmerkingen,
         boek, boekUitgever, boekStatus, boekDatumDate) in cursor:

        # Convert the datum to a string
        boekDatumStr = ""
        if boekDatumDate:
            boekDatumStr = boekDatumDate.strftime("%Y-%m-%d")

        if titel:
            print('"', titel.replace('"', '""'), '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if titelAuteurs:
            print('"', titelAuteurs.replace('"', '""'), '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if titelAuteur:
            print('"', titelAuteur.replace('"', '""'), '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if titelJaarInt:
            print('"', str(titelJaarInt), '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if titelType:
            print('"', titelType, '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if titelOnderwerp:
            print('"', titelOnderwerp, '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if titelVorm:
            print('"', titelVorm, '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if titelTaal:
            print('"', titelTaal, '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if titelOpmerkingen:
            print('"', titelOpmerkingen.replace('"', '""'), '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if boek:
            print('"', boek.replace('"', '""'), '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if boekUitgever:
            print('"', boekUitgever.replace('"', '""'), '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if boekStatus:
            print('"', boekStatus, '"', sep='', end='', file=boekenTitelCsvFile)

        print(',', end='', file=boekenTitelCsvFile)
        if boekDatumDate:
            print('"', boekDatumDate.strftime("%d-%m-%Y"), '"', sep='', end='', file=boekenTitelCsvFile)

        # Finish with newline
        print('', file=boekenTitelCsvFile)

    if args.outputPath:
        boekenTitelCsvFile.close()

    cursor.close()
    mysqlConnection.close()

except configparser.Error as configParserError:
    print("Configparser error:", configParserError)
except mysql.connector.Error as mysqlConnectionError:
    if mysqlConnectionError.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with user name:", mysqlConnectorConfig['user'],
              "or password:", mysqlConnectorConfig['password'])
    elif mysqlConnectionError.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database", mysqlConnectorConfig['database'], "does not exist")
    else:
        print("MySQL error:", mysqlConnectionError)
    sys.exit(1)
else:
    if args.outputPath:
        print("CSV file", args.outputPath, "successfully generated")
