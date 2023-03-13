import sqlite3
import re



def operations_report(con):
    cursor = con.cursor()
    cursor.execute('''
    CREATE TABLE if not exists REP_FRAUD (
        event_dt datetime,
        passport varchar(128),
        fio varchar(128),
        phone varchar(128),
        event_type varchar(128),
        report_dt datetime default current_timestamp
        )
    ''')


def passport_cheat(date, con):
    new_date = re.sub(r'(\d{2})(\d{2})(\d{4})', r'\3-\2-\1', date)
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE if not exists STG_PASSPORT_CHEAT as
            SELECT
                client_id,
                last_name,
                first_name,
                patronymic,
                passport_num,
                phone
            FROM DWH_DIM_CLIENTS
            WHERE COALESCE(passport_valid_to, '2999-12-31') < ?
            OR passport_num in (SELECT passport_num FROM DWH_FACT_PASSPORT_BLACKLIST);
        ''', [new_date])

    cursor.executescript('''
            CREATE TABLE if not exists STG_UNION_TABLE as
                SELECT
                    t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic as fio,
                    t1.passport_num,
                    t1.phone,
                    t4.trans_date
                FROM STG_PASSPORT_CHEAT t1
                LEFT JOIN DWH_DIM_ACCOUNTS t2
                ON t1.client_id = t2.client
                LEFT JOIN DWH_DIM_CARDS t3
                ON t2.account_num = t3.account_num
                LEFT JOIN DWH_FACT_TRANSACTIONS t4
                ON t3.card_num = t4.card_num
                LEFT JOIN DWH_FACT_PASSPORT_BLACKLIST t5
                ON t1.passport_num = t5.passport_num;


            INSERT INTO REP_FRAUD (
                event_dt, passport, fio, phone, event_type, report_dt
            ) SELECT
                trans_date,
                passport_num,
                fio,
                phone,
                'passport cheat',
                datetime('now')
            FROM STG_UNION_TABLE;
    ''')

    cursor.executescript('''
        DROP TABLE if exists STG_PASSPORT_CHEAT;
        DROP TABLE if exists STG_UNION_TABLE;
    ''')

    con.commit()

def account_cheat(date, con):
    new_date = re.sub(r'(\d{2})(\d{2})(\d{4})', r'\3-\2-\1', date)
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE if not exists STG_ACCOUNT_CHEAT as
            SELECT
                t1.account_num,
                t2.last_name,
                t2.first_name,
                t2.patronymic,
                t2.passport_num,
                t2.phone
            FROM DWH_DIM_ACCOUNTS t1
            LEFT JOIN DWH_DIM_CLIENTS t2
            ON t1.client = t2.client_id
            WHERE t1.valid_to < ?
        ''', [new_date])

    cursor.executescript('''
            CREATE TABLE if not exists STG_UNION_TABLE as
                SELECT
                    t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic as fio,
                    t1.passport_num,
                    t1.phone,
                    t3.trans_date
                FROM STG_ACCOUNT_CHEAT t1
                LEFT JOIN DWH_DIM_CARDS t2
                ON t1.account_num = t2.account_num
                LEFT JOIN DWH_FACT_TRANSACTIONS t3
                ON t2.card_num = t3.card_num;

            INSERT INTO REP_FRAUD (
                event_dt, passport, fio, phone, event_type, report_dt
            ) SELECT
                trans_date,
                passport_num,
                fio,
                phone,
                'account cheat',
                datetime('now')
            FROM STG_UNION_TABLE;
    ''')

    cursor.executescript('''
        DROP TABLE if exists STG_ACCOUNT_CHEAT;
        DROP TABLE if exists STG_UNION_TABLE;
    ''')

    con.commit()


def city_cheat(con):
    cursor = con.cursor()
    cursor.executescript('''
        CREATE TABLE if not exists STG_TRANSACTIONS_TERMINALS as
            SELECT
                t1.card_num,
                count(distinct t2.terminal_city) as cnt_city
            FROM DWH_FACT_TRANSACTIONS t1
            LEFT JOIN DWH_DIM_TERMINALS_HIST t2
            ON t1.terminal = t2.terminal_id
            group by t1.card_num
            having count(distinct t2.terminal_city) > 1;

        CREATE TABLE if not exists STG_TRANSACTIONS_LAG as
            SELECT
                t1.card_num,
                t3.terminal_city,
                lag(t3.terminal_city) over (partition by t1.card_num) as lag_city,
                t2.trans_date,
                lag(t2.trans_date) over (partition by t1.card_num) as lag_time,
                t2.oper_result
            FROM STG_TRANSACTIONS_TERMINALS t1
            LEFT JOIN DWH_FACT_TRANSACTIONS t2
            ON t1.card_num = t2.card_num
            LEFT JOIN DWH_DIM_TERMINALS_HIST t3
            ON t2.terminal = t3.terminal_id;

        CREATE TABLE if not exists STG_TRANSACTIONS_CHEAT as
            SELECT
                card_num,
                min(trans_date) as date_cheat
            FROM STG_TRANSACTIONS_LAG
            WHERE ((JULIANDAY(trans_date) - JULIANDAY(lag_time)) * 24 * 60) < 60
            AND terminal_city <> lag_city
            AND oper_result = 'SUCCESS'
            GROUP BY card_num;

        CREATE TABLE if not exists STG_UNION_TABLE as
            SELECT
                t4.last_name || ' ' || t4.first_name || ' ' || t4.patronymic as fio,
                t4.passport_num,
                t4.phone,
                t1.date_cheat
            FROM STG_TRANSACTIONS_CHEAT t1
            LEFT JOIN DWH_DIM_CARDS t2
            ON t1.card_num = t2.card_num
            LEFT JOIN DWH_DIM_ACCOUNTS t3
            ON t2.account_num = t3.account_num
            LEFT JOIN DWH_DIM_CLIENTS t4
            ON t3.client = t4.client_id;

        INSERT INTO REP_FRAUD (
            event_dt, passport, fio, phone, event_type, report_dt
        ) SELECT
            date_cheat,
            passport_num,
            fio,
            phone,
            'city cheat',
            datetime('now')
        FROM STG_UNION_TABLE;
        ''')

    cursor.executescript('''
        DROP TABLE if exists STG_TRANSACTIONS_TERMINALS;
        DROP TABLE if exists STG_TRANSACTIONS_LAG;
        DROP TABLE if exists STG_TRANSACTIONS_CHEAT;
        DROP TABLE if exists STG_UNION_TABLE;
    ''')
    con.commit()

def selection_of_the_amount(con):
    cursor = con.cursor()
    cursor.executescript('''
        CREATE TABLE if not exists STG_TRANSACTIONS_TYPE as
            SELECT
                trans_date,
                oper_result,
                amt,
                card_num,
                lag(trans_date, 3) over (partition by card_num order by trans_date) as lag_time,
                lag(oper_result) over (partition by card_num order by trans_date) as lag_result_1,
                lag(oper_result, 2) over (partition by card_num order by trans_date) as lag_result_2,
                lag(oper_result, 3) over (partition by card_num order by trans_date) as lag_result_3,
                lag(amt) over (partition by card_num order by trans_date) as lag_amt_1,
                lag(amt, 2) over (partition by card_num order by trans_date) as lag_amt_2,
                lag(amt, 3) over (partition by card_num order by trans_date) as lag_amt_3
            FROM DWH_FACT_TRANSACTIONS
            WHERE oper_type = 'WITHDRAW'
            OR oper_type = 'PAYMENT';

        CREATE TABLE if not exists STG_TRANSACTIONS_LAG as
            SELECT
                trans_date,
                card_num
            FROM STG_TRANSACTIONS_TYPE
            WHERE oper_result = 'SUCCESS'
            AND lag_result_1 = 'REJECT'
            AND lag_result_2 = 'REJECT'
            AND lag_result_3 = 'REJECT'
            AND cast(replace(lag_amt_3, ',', '.') as float) > cast(replace(lag_amt_2, ',', '.') as float)
            AND cast(replace(lag_amt_2, ',', '.') as float) > cast(replace(lag_amt_1, ',', '.') as float)
            AND cast(replace(lag_amt_1, ',', '.') as float) > cast(replace(amt, ',', '.') as float)
            AND ((JULIANDAY(trans_date) - JULIANDAY(lag_time)) * 24 * 60) < 20;

        CREATE TABLE if not exists STG_SELECTION_OF_THE_AMOUNT as
            SELECT
                t4.last_name || ' ' || t4.first_name || ' ' || t4.patronymic as fio,
                t4.passport_num,
                t4.phone,
                t1.trans_date
            FROM STG_TRANSACTIONS_LAG t1
            LEFT JOIN DWH_DIM_CARDS t2
            ON t1.card_num = t2.card_num
            LEFT JOIN DWH_DIM_ACCOUNTS t3
            ON t2.account_num = t3.account_num
            LEFT JOIN DWH_DIM_CLIENTS t4
            ON t3.client = t4.client_id;

        INSERT INTO REP_FRAUD (
            event_dt, passport, fio, phone, event_type, report_dt
        ) SELECT
            trans_date,
            passport_num,
            fio,
            phone,
            'selection of the amount',
            datetime('now')
        FROM STG_SELECTION_OF_THE_AMOUNT;
    ''')

    cursor.executescript('''
        DROP TABLE if exists STG_TRANSACTIONS_TYPE;
        DROP TABLE if exists STG_TRANSACTIONS_LAG;
        DROP TABLE if exists STG_SELECTION_OF_THE_AMOUNT;
    ''')

    con.commit()
