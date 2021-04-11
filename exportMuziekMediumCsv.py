#!/usr/bin/env python3

"""exportMuziekMediumCsv.py: Export table medium of database muziek as CSV"""

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
classicalGenre = "classical"
parser.add_argument("-g", "--genre", help="genre (default " + classicalGenre + ")",
                    choices=[classicalGenre, "rest"], default=classicalGenre)
parser.add_argument("-o", "--outputPath", help="CSV output file path (default none)")

args = parser.parse_args()

# Setup the WHERE clause on genre
whereClause = "WHERE medium.genre_id "
whereClause += "=" if args.genre == classicalGenre else "!="
whereClause += " 1 "

# Check if the database configuration file exists
if not os.path.isfile(args.configPath):
    print("Configuration file", args.configPath, "not found")
    exit(1)

# Read the database configuration file
databaseConfig = configparser.ConfigParser()
databaseConfig.read(args.configPath)
mysqlConnectorConfig = {
    'host': databaseConfig['connection']['host'],
    'user': databaseConfig['muziek']['user'],
    'password': databaseConfig['muziek']['password'],
    'database': databaseConfig['muziek']['database'],
    'raise_on_warnings': databaseConfig.getboolean('general', 'raise_on_warnings')
}

try:
    # Setup the MySQL query on table medium of database muziek
    query = ("SELECT "
             "medium.medium_titel, medium.uitvoerenden, genre.genre, subgenre.subgenre, "
             "medium_type.medium_type, medium_status.medium_status, "
             "label.label, medium.label_nummer, "
             "opslag.opslag, medium.medium_datum, medium.opmerkingen "
             "FROM medium "
             "LEFT JOIN genre ON genre.genre_id = medium.genre_id "
             "LEFT JOIN subgenre ON subgenre.subgenre_id = medium.subgenre_id "
             "LEFT JOIN medium_type ON medium_type.medium_type_id = medium.medium_type_id "
             "LEFT JOIN medium_status ON medium_status.medium_status_id = medium.medium_status_id "
             "LEFT JOIN label ON label.label_id = medium.label_id "
             "LEFT JOIN opslag ON opslag.opslag_id = medium.opslag_id " +
             whereClause +
             "ORDER BY medium.medium_titel")

    # Setup a connection to the MySQL database
    mysqlConnection = mysql.connector.connect(**mysqlConnectorConfig)

    # Execute the query
    cursor = mysqlConnection.cursor()
    cursor.execute(query)

    # Open the output file
    muziekMediumCsvFile = open(args.outputPath, mode='w', encoding='iso-8859-1') if args.outputPath else sys.stdout

    # Print the CSV header
    print("Medium Titel,Uitvoerenden,", sep='', end='', file=muziekMediumCsvFile)
    if args.genre == classicalGenre:
        print("Sub-genre,", sep='', end='', file=muziekMediumCsvFile)
    else:
        print("Genre,", sep='', end='', file=muziekMediumCsvFile)
    print("Type,Status,Label,Label Nummer,Datum,Opslag", file=muziekMediumCsvFile)

    # Define a date object with datetime
    mediumDatumDate = datetime.date(1, 1, 1)

    for (mediumTitel, uitvoerenden, mediumGenre, mediumSubgenre, mediumType, mediumStatus,
         label, labelNummer, opslag, mediumDatumDate, opmerkingen) in cursor:
        # Convert the datum to a string
        mediumDatumStr = ""
        if mediumDatumDate:
            mediumDatumStr = mediumDatumDate.strftime("%Y-%m-%d")

        if mediumTitel:
            print('"', mediumTitel.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if uitvoerenden:
            print('"', uitvoerenden.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if args.genre == classicalGenre:
            if mediumSubgenre:
                print('"', mediumSubgenre.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)
        else:
            if mediumGenre:
                print('"', mediumGenre, '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if mediumType:
            print('"', mediumType, '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if mediumStatus:
            print('"', mediumStatus, '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if label:
            print('"', label.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if labelNummer:
            print('"', labelNummer.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if mediumDatumStr:
            print('"', mediumDatumStr, '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if opslag:
            print('"', opslag.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

        print('', file=muziekMediumCsvFile)

    if args.outputPath:
        muziekMediumCsvFile.close()

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
