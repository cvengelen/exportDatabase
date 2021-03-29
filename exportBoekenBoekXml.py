#!/usr/bin/env python3

"""exportBoekenBoekXml.py: Export table boek of database boeken as XML"""

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
defaultStatusFilter = "boek.status_id != 10"
parser.add_argument("-s", "--statusFilter", help="boek status filter (default \"" + defaultStatusFilter + "\")",
                    default=defaultStatusFilter)
parser.add_argument("-t", "--typeFilter", help="boek type filter (default none)")
defaultOutputPath = "boekenBoek.xml"
parser.add_argument("-o", "--outputPath", help="XML output file path (default " + defaultOutputPath + ")",
                    default=defaultOutputPath)
args = parser.parse_args()

# Setup the WHERE clause on boek status and/or type
whereClause = ""
if args.statusFilter or args.typeFilter:
    whereClause = "WHERE "
    if args.statusFilter:
        whereClause += "(" + args.statusFilter + ")"
        if args.typeFilter:
            whereClause += " AND "
    if args.typeFilter:
        whereClause += "(" + args.typeFilter + ")"
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
    'user': databaseConfig['boeken']['user'],
    'password': databaseConfig['boeken']['password'],
    'database': databaseConfig['boeken']['database'],
    'raise_on_warnings': databaseConfig.getboolean('connection', 'raise_on_warnings')
}

try:
    # Setup the MySQL query on table boek of database boeken
    query = ("SELECT boek.boek, type.type, "
             "uitgever.uitgever, uitgever.isbn_1, uitgever.isbn_2, boek.isbn_3, boek.isbn_4, "
             "status.status, label.label, boek.datum, boek.opmerkingen "
             "FROM boek "
             "LEFT JOIN type ON type.type_id = boek.type_id "
             "LEFT JOIN uitgever ON uitgever.uitgever_id = boek.uitgever_id "
             "LEFT JOIN status ON status.status_id = boek.status_id "
             "LEFT JOIN label ON label.label_id = boek.label_id " +
             whereClause +
             "ORDER BY label.label, boek.boek")

    # Setup a connection to the MySQL database
    mysqlConnection = mysql.connector.connect(**mysqlConnectorConfig)

    # Execute the query
    cursor = mysqlConnection.cursor()
    cursor.execute(query)

    # for (boek, type, uitgever, isbn_1, isbn_2, isbn_3, isbn_4, status, label, datum, opmerkingen) in cursor:
    #    print("{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}".format(
    #        boek, type, uitgever, isbn_1, isbn_2, isbn_3, isbn_4, status, label, datum, opmerkingen))

    # Setup the XML structure
    databaseElement = cElementTree.Element("database")
    boekSubElement = cElementTree.SubElement(databaseElement, "boek")

    # Define a date object with datetime
    datumDate = datetime.date(1, 1, 1)

    for (boek, boek_type, uitgever, isbn_1, isbn_2, isbn_3, isbn_4,
         status, label, datumDate, opmerkingen) in cursor:
        # Convert the datum to a string
        datumStr = ""
        if datumDate is not None:
            datumStr = datumDate.strftime("%Y-%m-%d")

        # Store the data as fields of a row
        rowSubElement = cElementTree.SubElement(boekSubElement, "row")
        cElementTree.SubElement(rowSubElement, "boek").text = boek
        cElementTree.SubElement(rowSubElement, "type").text = boek_type
        cElementTree.SubElement(rowSubElement, "uitgever").text = uitgever
        cElementTree.SubElement(rowSubElement, "isbn_1").text = isbn_1
        cElementTree.SubElement(rowSubElement, "isbn_2").text = isbn_2
        cElementTree.SubElement(rowSubElement, "isbn_3").text = isbn_3
        cElementTree.SubElement(rowSubElement, "isbn_4").text = isbn_4
        cElementTree.SubElement(rowSubElement, "status").text = status
        cElementTree.SubElement(rowSubElement, "label").text = label
        cElementTree.SubElement(rowSubElement, "datum").text = datumStr
        cElementTree.SubElement(rowSubElement, "opmerkingen").text = opmerkingen

    boekenBoekXmlFile = open(args.outputPath, mode='w', encoding='utf8', errors="xmlcharrefreplace")

    # Print XML file header
    print("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>", file=boekenBoekXmlFile)
    print("<?xml-stylesheet type=\"text/xsl\" href=\"boekenBoek.xsl\"?>", file=boekenBoekXmlFile)

    # Write the data as XML
    databaseElementTree = cElementTree.ElementTree(databaseElement)
    databaseElementTree.write(file_or_filename=boekenBoekXmlFile, encoding="utf-8")

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
