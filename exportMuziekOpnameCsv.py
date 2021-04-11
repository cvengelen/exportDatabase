#!/usr/bin/env python3

"""exportMuziekOpnameCsv.py: Export table opname of database muziek as CSV"""

__author__ = "Chris van Engelen"
__copyright__ = "Copyright 2021"

import sys
import os.path
import mysql.connector
from mysql.connector import errorcode
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

# Setup the ORDER clause on genre
if args.genre == classicalGenre:
    orderClause = "ORDER BY persoon.persoon, type.type, opus.opus_titel, opus.opus_nummer, musici.musici"
else:
    orderClause = "ORDER BY opus.opus_titel, musici.musici"

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
             "opus.opus_titel, opus.opus_nummer, type.type, tijdperk.tijdperk, "
             "persoon.persoon as componist, musici.musici, genre.genre, "
             "medium_type.medium_type, medium_status.medium_status, " 
             "label.label, medium.label_nummer, medium.medium_titel "
             "FROM opname "
             "LEFT JOIN opus ON opus.opus_id = opname.opus_id "
             "LEFT JOIN type ON type.type_id = opus.type_id "
             "LEFT JOIN tijdperk ON tijdperk.tijdperk_id = opus.tijdperk_id "
             "LEFT JOIN componisten_persoon ON componisten_persoon.componisten_id = opus.componisten_id "
             "LEFT JOIN persoon ON persoon.persoon_id = componisten_persoon.persoon_id "
             "LEFT JOIN musici ON musici.musici_id = opname.musici_id "
             "LEFT JOIN genre ON genre.genre_id = opus.genre_id "
             "LEFT JOIN medium ON medium.medium_id = opname.medium_id "
             "LEFT JOIN medium_type ON medium.medium_type_id = medium_type.medium_type_id "
             "LEFT JOIN medium_status ON medium.medium_status_id = medium_status.medium_status_id "
             "LEFT JOIN label ON medium.label_id = label.label_id " +
             whereClause + orderClause)

    # Setup a connection to the MySQL database
    mysqlConnection = mysql.connector.connect(**mysqlConnectorConfig)

    # Execute the query
    cursor = mysqlConnection.cursor()
    cursor.execute(query)

    # Open the output file
    muziekMediumCsvFile = open(args.outputPath, mode='w', encoding='iso-8859-1') if args.outputPath else sys.stdout

    # Print the CSV header
    if args.genre == classicalGenre:
        print("Componist,Titel,Opus,Type,Tijdperk,", end='', file=muziekMediumCsvFile)
    else:
        print("Titel,Genre,Componist,", end='', file=muziekMediumCsvFile)
    print("Musici,Medium,Status,Label,Label Nummer,Medium Titel", file=muziekMediumCsvFile)

    for (opusTitel, opusNummer, opusType, opusTijdperk, componist, musici, opusGenre,
         mediumType, mediumStatus, mediumLabel, mediumLabelNummer, mediumTitel) in cursor:

        if args.genre == classicalGenre:
            if componist:
                print('"', componist.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)
            print(',', end='', file=muziekMediumCsvFile)
            if opusTitel:
                print('"', opusTitel.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)
            print(',', end='', file=muziekMediumCsvFile)
            if opusNummer:
                print('"', opusNummer.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)
            print(',', end='', file=muziekMediumCsvFile)
            if opusType:
                print('"', opusType, '"', sep='', end='', file=muziekMediumCsvFile)
            print(',', end='', file=muziekMediumCsvFile)
            if opusTijdperk:
                print('"', opusTijdperk, '"', sep='', end='', file=muziekMediumCsvFile)
        else:
            if opusTitel:
                print('"', opusTitel.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)
            print(',', end='', file=muziekMediumCsvFile)
            if opusGenre:
                print('"', opusGenre, '"', sep='', end='', file=muziekMediumCsvFile)
            print(',', end='', file=muziekMediumCsvFile)
            if componist:
                print('"', componist.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if musici:
            print('"', musici.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if mediumType:
            print('"', mediumType, '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if mediumStatus:
            print('"', mediumStatus, '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if mediumLabel:
            print('"', mediumLabel.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if mediumLabelNummer:
            print('"', mediumLabelNummer.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

        print(',', end='', file=muziekMediumCsvFile)
        if mediumTitel:
            print('"', mediumTitel.replace('"', '""'), '"', sep='', end='', file=muziekMediumCsvFile)

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
