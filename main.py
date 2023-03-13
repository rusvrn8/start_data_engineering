import sqlite3
import sys
from py_scripts import load, report


def main():
    conn = sqlite3.connect('bank.db')
    load.bank(conn)
    report.operations_report(conn)
    date = ''
    try:
        date = load.search_file_date()
    except Exception as e:
        print('Error: ' + str(e))
        sys.exit()
    if date != '':
        load.transaction(date, conn)
        load.passport_blacklist(date, conn)
        load.increment_load(date, conn)
        report.account_cheat(date, conn)
        report.passport_cheat(date, conn)
        report.city_cheat(conn)
        report.selection_of_the_amount(conn)
    load.show_data('REP_FRAUD', conn)

main()
