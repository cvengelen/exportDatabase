#!/usr/bin/env python3

"""exportMuziekMediumXml.py: Export table medium of database muziek as XML"""

__author__ = "Chris van Engelen"
__copyright__ = "Copyright 2021"

import sys
import os.path
import mysql.connector
from mysql.connector import errorcode
import xml.etree.cElementTree as cElementTree
import datetime
import argparse
import configparser

# Process command line arguments
parser = argparse.ArgumentParser()
defaultConfigPath = "database.ini"
parser.add_argument("-c", "--configPath", help="database configuration file path (default " + defaultConfigPath + ")",
                    default=defaultConfigPath)
defaultStatusFilter = "(medium.medium_status_id != 1) AND (medium.medium_status_id != 9)"
parser.add_argument("-s", "--statusFilter", help="medium status filter (default \"" + defaultStatusFilter + "\")",
                    default=defaultStatusFilter)
parser.add_argument("-g", "--genreFilter", help="muziek genre filter (default none)")
defaultOutputPath = "muziekMedium.xml"
parser.add_argument("-o", "--outputPath", help="XML output file path (default " + defaultOutputPath + ")",
                    default=defaultOutputPath)
defaultXslPath = "muziekMedium.xsl"
parser.add_argument("-x", "--xslPath", help="XSL file path (default " + defaultXslPath + ")",
                    default=defaultXslPath)

args = parser.parse_args()

# Setup the WHERE clause on muziek status and/or genre
whereClause = ""
if args.statusFilter or args.genreFilter:
    whereClause = "WHERE "
    if args.statusFilter:
        whereClause += "(" + args.statusFilter + ")"
        if args.genreFilter:
            whereClause += " AND "
    if args.genreFilter:
        whereClause += "(" + args.genreFilter + ")"
    whereClause += " "

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
             "ORDER BY opslag.opslag, medium.subgenre_id, medium.medium_titel")

    # Setup a connection to the MySQL database
    mysqlConnection = mysql.connector.connect(**mysqlConnectorConfig)

    # Execute the query
    cursor = mysqlConnection.cursor()
    cursor.execute(query)

    # Setup the XML structure
    databaseElement = cElementTree.Element("muziek")
    muziekSubElement = cElementTree.SubElement(databaseElement, "medium")

    # Define a date object with datetime
    mediumDatumDate = datetime.date(1, 1, 1)

    for (mediumTitel, uitvoerenden, mediumGenre, mediumSubgenre, mediumType, mediumStatus,
         label, labelNummer, opslag, mediumDatumDate, opmerkingen) in cursor:
        # Convert the datum to a string
        mediumDatumStr = ""
        if mediumDatumDate is not None:
            mediumDatumStr = mediumDatumDate.strftime("%Y-%m-%d")

        # Store the data as fields of a row
        rowSubElement = cElementTree.SubElement(muziekSubElement, "row")
        cElementTree.SubElement(rowSubElement, "medium_titel").text = mediumTitel
        cElementTree.SubElement(rowSubElement, "uitvoerenden").text = uitvoerenden
        cElementTree.SubElement(rowSubElement, "genre").text = mediumGenre
        cElementTree.SubElement(rowSubElement, "subgenre").text = mediumSubgenre
        cElementTree.SubElement(rowSubElement, "medium_type").text = mediumType
        cElementTree.SubElement(rowSubElement, "medium_status").text = mediumStatus
        cElementTree.SubElement(rowSubElement, "label").text = label
        cElementTree.SubElement(rowSubElement, "label_nummer").text = labelNummer
        cElementTree.SubElement(rowSubElement, "opslag").text = opslag
        cElementTree.SubElement(rowSubElement, "medium_datum").text = mediumDatumStr
        cElementTree.SubElement(rowSubElement, "opmerkingen").text = opmerkingen

    muziekMediumXmlFile = open(args.outputPath, mode='w', encoding='utf8', errors="xmlcharrefreplace")

    # Print XML file header
    print("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>", file=muziekMediumXmlFile)
    print("<?xml-stylesheet type=\"text/xsl\" href=\"{}\"?>".format(args.xslPath), file=muziekMediumXmlFile)

    # Write the data as XML
    databaseElementTree = cElementTree.ElementTree(databaseElement)
    databaseElementTree.write(file_or_filename=muziekMediumXmlFile, encoding="utf-8")

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
    print("XML file", args.outputPath, "successfully generated")
