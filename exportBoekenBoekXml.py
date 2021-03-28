#!/usr/bin/env python3

"""exportBoekenBoekXml.py: Export table boek of database boeken as XML"""

__author__ = "Chris van Engelen"
__copyright__ = "Copyright 2021"

import sys
import mysql.connector
import xml.etree.cElementTree as cElementTree
import datetime
import argparse

from mysql.connector import errorcode

config = {
  'user': 'boeken',
  'password': 'TtBdbCve',
  'host': '127.0.0.1',
  'database': 'boeken',
  'raise_on_warnings': True
}

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--typeFilter", help="boek type filter")
parser.add_argument("-o", "--outputPath", help="XML output file path", default="boekenBoek.xml")
args = parser.parse_args()

typeFilter = ""
if args.typeFilter:
    typeFilter = "AND (" + args.typeFilter + ") "

try:
    cnx = mysql.connector.connect(**config)

    cursor = cnx.cursor()

    query = ("SELECT boek.boek, type.type, "
             "uitgever.uitgever, uitgever.isbn_1, uitgever.isbn_2, boek.isbn_3, boek.isbn_4, "
             "status.status, label.label, boek.datum, boek.opmerkingen "
             "FROM boek "
             "LEFT JOIN type ON type.type_id = boek.type_id "
             "LEFT JOIN uitgever ON uitgever.uitgever_id = boek.uitgever_id "
             "LEFT JOIN status ON status.status_id = boek.status_id "
             "LEFT JOIN label ON label.label_id = boek.label_id WHERE (boek.status_id != 10) " + typeFilter +
             "ORDER BY label.label, boek.boek")

    cursor.execute(query)

    # for (boek, type, uitgever, isbn_1, isbn_2, isbn_3, isbn_4, status, label, datum, opmerkingen) in cursor:
    #    print("{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}".format(
    #        boek, type, uitgever, isbn_1, isbn_2, isbn_3, isbn_4, status, label, datum, opmerkingen))

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
    cnx.close()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)

    cnx.close()
    sys.exit(1)
else:
    cnx.close()
