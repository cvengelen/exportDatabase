#!/usr/bin/env python3

"""exportMuziekOpnameXml.py: Export table opname of database muziek as XML"""

__author__ = "Chris van Engelen"
__copyright__ = "Copyright 2021"

import sys
import os.path
import mysql.connector
from mysql.connector import errorcode
import xml.etree.cElementTree as cElementTree
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
parser.add_argument("-o", "--outputPath", help="XML output file path (default none)")
defaultXslPath = "muziekOpname.xsl"
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
    # Setup the MySQL query on table opname of database muziek
    query = ("SELECT "
             "opus.opus_titel, opus.opus_nummer, genre.genre, type.type, "
             "componisten.componisten, persoon.persoon as componist, "
             "musici.musici, "
             "opname_datum.opname_datum, opname_plaats.opname_plaats, producers.producers, "
             "medium.medium_titel "
             "FROM opname "
             "LEFT JOIN opus ON opus.opus_id = opname.opus_id "
             "LEFT JOIN genre ON genre.genre_id = opus.genre_id "
             "LEFT JOIN type ON type.type_id = opus.type_id "
             "LEFT JOIN componisten ON componisten.componisten_id = opus.componisten_id "
             "LEFT JOIN componisten_persoon ON componisten_persoon.componisten_id = opus.componisten_id "
             "LEFT JOIN persoon ON persoon.persoon_id = componisten_persoon.persoon_id "
             "LEFT JOIN musici ON musici.musici_id = opname.musici_id "
             "LEFT JOIN opname_datum ON opname_datum.opname_datum_id = opname.opname_datum_id "
             "LEFT JOIN opname_plaats ON opname_plaats.opname_plaats_id = opname.opname_plaats_id "
             "LEFT JOIN producers ON producers.producers_id = opname.producers_id "
             "LEFT JOIN medium ON medium.medium_id = opname.medium_id " +
             whereClause +
             "ORDER BY persoon.persoon, opus.opus_titel, musici.musici")

    # Setup a connection to the MySQL database
    mysqlConnection = mysql.connector.connect(**mysqlConnectorConfig)

    # Execute the query
    cursor = mysqlConnection.cursor()
    cursor.execute(query)

    # Setup the XML structure
    databaseElement = cElementTree.Element("muziek")
    muziekSubElement = cElementTree.SubElement(databaseElement, "opname")

    for (opusTitel, opusNummer, opusGenre, opusType, componisten, componist, musici,
         opnameDatum, opnamePlaats, producers, mediumTitel) in cursor:

        # Store the data as fields of a row
        rowSubElement = cElementTree.SubElement(muziekSubElement, "row")
        cElementTree.SubElement(rowSubElement, "opus_titel").text = opusTitel
        cElementTree.SubElement(rowSubElement, "opus_nummer").text = opusNummer
        cElementTree.SubElement(rowSubElement, "genre").text = opusGenre
        cElementTree.SubElement(rowSubElement, "type").text = opusType
        cElementTree.SubElement(rowSubElement, "componisten").text = componisten
        cElementTree.SubElement(rowSubElement, "componist").text = componist
        cElementTree.SubElement(rowSubElement, "musici").text = musici
        cElementTree.SubElement(rowSubElement, "opname_datum").text = opnameDatum
        cElementTree.SubElement(rowSubElement, "opname_plaats").text = opnamePlaats
        cElementTree.SubElement(rowSubElement, "producers").text = producers
        cElementTree.SubElement(rowSubElement, "medium_titel").text = mediumTitel

    muziekOpnameXmlFile = open(args.outputPath, mode='w', encoding='utf8', errors="xmlcharrefreplace") \
        if args.outputPath else sys.stdout

    # Print XML file header
    print("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>", file=muziekOpnameXmlFile)
    print("<?xml-stylesheet type=\"text/xsl\" href=\"{}\"?>".format(args.xslPath), file=muziekOpnameXmlFile)

    # Write the data as XML
    databaseElementTree = cElementTree.ElementTree(databaseElement)
    databaseElementTree.write(file_or_filename=muziekOpnameXmlFile, encoding="utf-8")

    if args.outputPath:
        muziekOpnameXmlFile.close()

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
        print("XML file", args.outputPath, "successfully generated")
