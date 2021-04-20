#!/usr/bin/env python3

"""exportFinancienRubriekCsv.py: Export table rekening_mutatie of database financien per rubriek as CSV"""

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
defaultYear = 2020
parser.add_argument("-y", "--year", help="year (default )" + str(defaultYear) + ")", default=defaultYear)
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
    'user': databaseConfig['financien']['user'],
    'password': databaseConfig['financien']['password'],
    'database': databaseConfig['financien']['database'],
    'raise_on_warnings': databaseConfig.getboolean('general', 'raise_on_warnings')
}

try:
    # Setup a connection to the MySQL database
    mysqlConnection = mysql.connector.connect(**mysqlConnectorConfig)

    # A buffered cursor must be used, otherwise the query on the account mutations will give "Unread result found"
    # The buffered=True ensures that all rows of this query are fetched, and another query is possible. See:
    #   https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursorbuffered.html
    rubriekCursor = mysqlConnection.cursor(buffered=True)

    # Get rubriek ID and rubriek, exclude transfer to and from savings and stock accounts
    rubriekQuery = "SELECT rubriek_id, rubriek from rubriek where not rubriek like 'TRANSFER:%' order by rubriek"
    rubriekCursor.execute(rubriekQuery)

    # Open the output file
    financienRubriekCsvFile = open(args.outputPath, mode='w', encoding='iso-8859-1') if args.outputPath else sys.stdout

    # Print the CSV header
    print("Rubriek;ING Betaal in;ING Betaal uit;ING CreditCard in;ING CreditCard uit;Totaal in;Totaal uit;Totaal",
          file=financienRubriekCsvFile)

    # Initialise the sums
    sumIngBetaalIn = 0
    sumIngBetaalUit = 0
    sumCreditCardIn = 0
    sumCreditCardUit = 0
    sumTotalIn = 0
    sumTotalUit = 0
    sumTotal = 0

    # Date selection
    dateSelection = "datum between '" + str(args.year) + "-01-01' and  '" + str(args.year) + "-12-31'"

    # Get a cursor for the account mutations
    rekeningMutatieCursor = mysqlConnection.cursor()

    # Loop over rubriek
    for (rubriekId, rubriek) in rubriekCursor:
        # Get the mutation in/out for this rubriek and ING account
        rekeningMutatieQuery = "select sum(mutatie_in), sum(mutatie_uit) from rekening_mutatie where rubriek_id = " + \
                               str(rubriekId) + " and rekening_id = 1 and " + dateSelection
        rekeningMutatieCursor.execute(rekeningMutatieQuery)
        rekeningMutatieRow = rekeningMutatieCursor.fetchone()
        ingBetaalIn = rekeningMutatieRow[0] if rekeningMutatieRow[0] else 0
        ingBetaalUit = rekeningMutatieRow[1] if rekeningMutatieRow[1] else 0

        # Get the mutation in/out for this rubriek and credit card account
        rekeningMutatieQuery = "select sum(mutatie_in), sum(mutatie_uit) from rekening_mutatie where rubriek_id = " + \
                               str(rubriekId) + " and rekening_id = 39 and " + dateSelection
        rekeningMutatieCursor.execute(rekeningMutatieQuery)
        rekeningMutatieRow = rekeningMutatieCursor.fetchone()
        creditCardIn = rekeningMutatieRow[0] if rekeningMutatieRow[0] else 0
        creditCardUit = rekeningMutatieRow[1] if rekeningMutatieRow[1] else 0

        if ingBetaalIn != 0 or ingBetaalUit != 0 or creditCardIn != 0 or creditCardUit != 0:
            # Output the mutations for this rubriek
            totalIn = ingBetaalIn + creditCardIn
            totalUit = ingBetaalUit + creditCardUit
            total = totalIn - totalUit
            print(rubriek,
                  "{:.2f}".format(ingBetaalIn).replace('.', ','),
                  "{:.2f}".format(ingBetaalUit).replace('.', ','),
                  "{:.2f}".format(creditCardIn).replace('.', ','),
                  "{:.2f}".format(creditCardUit).replace('.', ','),
                  "{:.2f}".format(totalIn).replace('.', ','),
                  "{:.2f}".format(totalUit).replace('.', ','),
                  "{:.2f}".format(total).replace('.', ','),
                  sep=";", file=financienRubriekCsvFile)

            # Update the sums
            sumIngBetaalIn += ingBetaalIn
            sumIngBetaalUit += ingBetaalUit
            sumCreditCardIn += creditCardIn
            sumCreditCardUit += creditCardUit
            sumTotalIn += totalIn
            sumTotalUit += totalUit
            sumTotal += total

    # Output the sums of all mutations
    print("Totaal",
          "{:.2f}".format(sumIngBetaalIn).replace('.', ','),
          "{:.2f}".format(sumIngBetaalUit).replace('.', ','),
          "{:.2f}".format(sumCreditCardIn).replace('.', ','),
          "{:.2f}".format(sumCreditCardUit).replace('.', ','),
          "{:.2f}".format(sumTotalIn).replace('.', ','),
          "{:.2f}".format(sumTotalUit).replace('.', ','),
          "{:.2f}".format(sumTotal).replace('.', ','),
          sep=";", file=financienRubriekCsvFile)

    if args.outputPath:
        financienRubriekCsvFile.close()

    rekeningMutatieCursor.close()
    rubriekCursor.close()
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
