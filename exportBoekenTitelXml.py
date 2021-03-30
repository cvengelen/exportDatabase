#!/usr/bin/env python3

"""exportBoekenTitelXml.py: Export table titel of database boeken as XML"""

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
defaultOutputPath = "boekenTitel.xml"
parser.add_argument("-o", "--outputPath", help="XML output file path (default " + defaultOutputPath + ")",
                    default=defaultOutputPath)
defaultXslPath = "boekenTitel.xsl"
parser.add_argument("-x", "--xslPath", help="XSL file path (default " + defaultXslPath + ")",
                    default=defaultXslPath)
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
    'raise_on_warnings': databaseConfig.getboolean('general', 'raise_on_warnings')
}

try:
    # Setup the MySQL query on table titel of database boeken
    query = ("SELECT titel.titel, auteurs.auteurs, persoon.persoon, titel.jaar, titel.opmerkingen, "
             "type.type, onderwerp.onderwerp, vorm.vorm, taal.taal, boek.boek, "
             "uitgever.uitgever, status.status, boek.datum "
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
             whereClause +
             "ORDER BY persoon.persoon, titel.titel")

    # Setup a connection to the MySQL database
    mysqlConnection = mysql.connector.connect(**mysqlConnectorConfig)

    # Execute the query
    cursor = mysqlConnection.cursor()
    cursor.execute(query)

    boekenElement = cElementTree.Element("boeken")
    titelSubElement = cElementTree.SubElement(boekenElement, "titel")

    # Define a date object with datetime
    datumDate = datetime.date(1, 1, 1)

    for (titel, auteurs, persoon, jaar, opmerkingen, titel_type, onderwerp, vorm, taal,
         boek, uitgever, status, datumDate) in cursor:
        # Convert the datum to a string
        datumStr = ""
        if datumDate is not None:
            datumStr = datumDate.strftime("%Y-%m-%d")

        # Convert jaar from int to str
        jaarStr = ""
        if jaar is not None:
            jaarStr = str(jaar)

        # Store the data as fields of a row
        rowSubElement = cElementTree.SubElement(titelSubElement, "row")
        cElementTree.SubElement(rowSubElement, "titel").text = titel
        cElementTree.SubElement(rowSubElement, "auteurs").text = auteurs
        cElementTree.SubElement(rowSubElement, "persoon").text = persoon
        cElementTree.SubElement(rowSubElement, "jaar").text = jaarStr
        cElementTree.SubElement(rowSubElement, "opmerkingen").text = opmerkingen
        cElementTree.SubElement(rowSubElement, "type").text = titel_type
        cElementTree.SubElement(rowSubElement, "onderwerp").text = onderwerp
        cElementTree.SubElement(rowSubElement, "vorm").text = vorm
        cElementTree.SubElement(rowSubElement, "taal").text = taal
        cElementTree.SubElement(rowSubElement, "boek").text = boek
        cElementTree.SubElement(rowSubElement, "uitgever").text = uitgever
        cElementTree.SubElement(rowSubElement, "status").text = status
        cElementTree.SubElement(rowSubElement, "datum").text = datumStr

    boekenTitelXmlFile = open(args.outputPath, mode='w', encoding='utf8', errors="xmlcharrefreplace")

    # Print XML file header
    print("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>", file=boekenTitelXmlFile)
    print("<?xml-stylesheet type=\"text/xsl\" href=\"{}\"?>".format(args.xslPath), file=boekenTitelXmlFile)

    # Write the data as XML
    boekenElementTree = cElementTree.ElementTree(boekenElement)
    boekenElementTree.write(file_or_filename=boekenTitelXmlFile, encoding="utf-8")

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
