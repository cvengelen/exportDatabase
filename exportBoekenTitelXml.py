#!/usr/bin/env python3

"""exportBoekenBoekXml.py: Export table titel of database boeken as XML"""

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
parser.add_argument("-o", "--outputPath", help="XML output file path", default="boekenTitel.xml")
args = parser.parse_args()

typeFilter = ""
if args.typeFilter:
    typeFilter = "AND (" + args.typeFilter + ") "

try:
    cnx = mysql.connector.connect(**config)

    cursor = cnx.cursor()

    query = ("SELECT titel.titel, persoon.persoon, titel.jaar, titel.opmerkingen, "
             "type.type, onderwerp.onderwerp, vorm.vorm, taal.taal, boek.boek, "
             "uitgever.uitgever, status.status, boek.datum "
             "FROM titel "
             "LEFT JOIN auteurs_persoon ON auteurs_persoon.auteurs_id = titel.auteurs_id "
             "LEFT JOIN persoon ON persoon.persoon_id = auteurs_persoon.persoon_id "
             "LEFT JOIN onderwerp ON onderwerp.onderwerp_id = titel.onderwerp_id "
             "LEFT JOIN vorm ON vorm.vorm_id = titel.vorm_id "
             "LEFT JOIN taal ON taal.taal_id = titel.taal_id "
             "LEFT JOIN boek on titel.boek_id = boek.boek_id "
             "LEFT JOIN type ON type.type_id = boek.type_id "
             "LEFT JOIN uitgever ON uitgever.uitgever_id = boek.uitgever_id "
             "LEFT JOIN status ON status.status_id = boek.status_id "
             "WHERE (boek.status_id != 10) " + typeFilter +
             "ORDER BY persoon.persoon, titel.titel")

    cursor.execute(query)

    boekenElement = cElementTree.Element("boeken")
    titelSubElement = cElementTree.SubElement(boekenElement, "titel")

    # Define a date object with datetime
    datumDate = datetime.date(1, 1, 1)

    for (titel, persoon, jaar, opmerkingen, titel_type, onderwerp, vorm, taal,
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
    print("<?xml-stylesheet type=\"text/xsl\" href=\"boekenTitel.xsl\"?>", file=boekenTitelXmlFile)

    # Write the data as XML
    boekenElementTree = cElementTree.ElementTree(boekenElement)
    boekenElementTree.write(file_or_filename=boekenTitelXmlFile, encoding="utf-8")

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
