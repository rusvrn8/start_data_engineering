import pandas as pd
import sqlite3
import os
import re


# Функция поиска даты в файле
def search_file_date():
    date = ''
    path = r'.\data'
    rez = sorted(os.listdir(path))
    for name in rez:
        search_date = re.search(r'\d{8}', name)
        date = search_date.group()
        break

    if date == '':
        raise Exception('Upload files not found')
    os.chdir(path)
    if not os.path.isfile('transactions_' + date + '.txt'):
        raise Exception('Transactions file not found')
    if not os.path.isfile('passport_blacklist_' + date + '.xlsx'):
        raise Exception('Passport_blacklist file not found')
    if not os.path.isfile('terminals_' + date + '.xlsx'):
        raise Exception('Terminals file not found')
    os.chdir(os.pardir)
    return date




# Загрузка данных из файла excel и перевод данных в таблицу sql
def excel_load_to_sql(path, name_table, con):
    df = pd.read_excel(path)
    df.to_sql(name_table, con = con, if_exists = 'replace', index = False)

# Загрузка данных из файла csv и перевод данных в таблицу sql
def csv_load_to_sql(path, name_table, con):
    df = pd.read_csv(path, sep = ';')
    df.to_sql(name_table, con = con, if_exists = 'replace', index = False)



# Загрузка данных из файла sql и создание таблиц
def sql_load(path, con):
    cursor = con.cursor()
    file = open(path, 'r', encoding = 'UTF-8')
    string = file.read()
    cursor.executescript(string)
    con.commit()


# Функция просмотра таблицы
def show_data(source, con):
    cursor = con.cursor()
    print('-_' * 20)
    print(source)
    print('-_' * 20)

    cursor.execute(f'select * from {source}')

    for row in cursor.fetchall():
        print(row)

    print('-_' * 20)

# Создаем базу данных Банк
def bank(con):
    sql_load('ddl_dml.sql', con)
    cursor = con.cursor()
    cursor.executescript('''
        DROP TABLE if exists DWH_DIM_CLIENTS;
        CREATE TABLE if not exists DWH_DIM_CLIENTS(
            client_id varchar(128) primary key,
            last_name varchar(128),
            first_name varchar(128),
            patronymic varchar(128),
            date_of_birth date,
            passport_num varchar(128),
            passport_valid_to date,
            phone varchar(128),
            create_dt date,
            update_dt date
        );

        INSERT INTO DWH_DIM_CLIENTS (
            client_id, last_name, first_name, patronymic, date_of_birth,
            passport_num, passport_valid_to, phone, create_dt, update_dt
        ) SELECT
            client_id, last_name, first_name, patronymic, date_of_birth,
            passport_num, passport_valid_to, phone, create_dt,
            update_dt FROM clients;

        DROP TABLE if exists DWH_DIM_ACCOUNTS;
        CREATE TABLE if not exists DWH_DIM_ACCOUNTS(
            account_num varchar(128) primary key,
            valid_to date,
            client varchar(128),
            create_dt date,
            update_dt date,
            FOREIGN KEY (client) REFERENCES DWH_DIM_CLIENTS (client_id)
        );

        INSERT INTO DWH_DIM_ACCOUNTS (
            account_num, valid_to, client, create_dt, update_dt
        ) SELECT
            account, valid_to, client, create_dt, update_dt FROM accounts;

        DROP TABLE if exists DWH_DIM_CARDS;
        CREATE TABLE if not exists DWH_DIM_CARDS(
            card_num varchar(128) primary key,
            account_num varchar(128),
            create_dt date,
            update_dt date,
            FOREIGN KEY (account_num) REFERENCES DWH_DIM_ACCOUNTS (account_num)
        );

        INSERT INTO DWH_DIM_CARDS (
            card_num, account_num, create_dt, update_dt
        ) SELECT
            card_num, account, create_dt, update_dt FROM cards;

    ''')

    cursor.executescript('''
        DROP TABLE if exists cards;
        DROP TABLE if exists accounts;
        DROP TABLE if exists clients
    ''')

    con.commit()

# Функция создания таблицы паспортов в черном списке
def passport_blacklist(date, con):
    file_path = r'data\passport_blacklist_' + date + '.xlsx'
    file_name = 'passport_blacklist_' + date + '.xlsx'
    excel_load_to_sql(file_path, 'STG_PASSPORT_BLACKLIST', con)
    cursor = con.cursor()
    cursor.executescript('''
        DROP TABLE if exists DWH_FACT_PASSPORT_BLACKLIST;

        CREATE TABLE if not exists DWH_FACT_PASSPORT_BLACKLIST(
            passport_num varchar(128),
            entry_dt date
        );

        INSERT INTO DWH_FACT_PASSPORT_BLACKLIST (
            passport_num, entry_dt
        ) SELECT
            passport, date FROM STG_PASSPORT_BLACKLIST;
        ''')

    cursor.execute('DROP TABLE if exists STG_PASSPORT_BLACKLIST')

    os.chdir('data')
    os.rename(file_name, file_name + '.backup')
    backup_file = file_name + '.backup'
    os.chdir(os.pardir)
    os.replace(r'data\\' + backup_file, r'archive\\' + backup_file)

    con.commit()

# Функция создания таблицы транзакций
def transaction(date, con):
    file_path = r'data\transactions_' + date + '.txt'
    file_name = 'transactions_' + date + '.txt'

    csv_load_to_sql(file_path, 'STG_TRANSACTIONS', con)
    cursor = con.cursor()
    cursor.executescript('''
        DROP TABLE if exists DWH_FACT_TRANSACTIONS;

        CREATE TABLE if not exists DWH_FACT_TRANSACTIONS (
            trans_id varchar(128),
            trans_date datetime,
            amt float,
            card_num varchar(128),
            oper_type varchar(128),
            oper_result varchar(128),
            terminal varchar(128),
            FOREIGN KEY (card_num) REFERENCES DWH_DIM_CARDS (card_num),
            FOREIGN KEY (terminal) REFERENCES DWH_DIM_TERMINALS (terminal_id)
        );

        INSERT INTO DWH_FACT_TRANSACTIONS (
            trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal
        ) SELECT
            transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal FROM STG_TRANSACTIONS;
        ''')

    cursor.execute('DROP TABLE if exists STG_TRANSACTIONS')

    os.chdir('data')
    os.rename(file_name, file_name + '.backup')
    backup_file = file_name + '.backup'
    os.chdir(os.pardir)
    os.replace(r'data\\' + backup_file, r'archive\\' + backup_file)

    con.commit()


# Функция создания таблицы терминалов
def init_terminals_hist(con):
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_TERMINALS_HIST(
            id integer primary key autoincrement,
            terminal_id varchar(128),
            terminal_type varchar(128),
            terminal_city varchar(128),
            terminal_address varchar(128),
            deleted_flg integer default 0,
            effective_from datetime default current_timestamp,
            effective_to datetime default (datetime ('2999-12-31 23:59:59'))
        )
    ''')
    cursor.execute('''
        CREATE VIEW if not exists V_TERMINALS as
            SELECT
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address
            FROM DWH_DIM_TERMINALS_HIST
            WHERE deleted_flg = 0
            AND current_timestamp between effective_from and effective_to
    ''')

# Функция новых записей
def create_new_rows(con):
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE NEW_STG_TERMINALS as
            SELECT
                t1.*
            FROM STG_TERMINALS t1
            LEFT JOIN V_TERMINALS t2
            ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id is null
    ''')

# Функция удаленных записей
def create_deleted_rows(con):
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE DELETED_STG_TERMINALS as
            SELECT
                t1.*
            FROM V_TERMINALS t1
            LEFT JOIN STG_TERMINALS t2
            ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id is null
    ''')

# Функция изменнеых записей
def create_changed_rows(con):
    cursor = con.cursor()
    cursor.execute('''
        CREATE TABLE CHANGED_STG_TERMINALS as
            SELECT
                t2.*
            FROM V_TERMINALS t1
            INNER JOIN STG_TERMINALS t2
            ON t1.terminal_id = t2.terminal_id
            AND (t1.terminal_type <> t2.terminal_type
            OR t1.terminal_city <> t2.terminal_city
            OR t1.terminal_address <> t2.terminal_address
            )
    ''')

# Функция обновления таблицы терминалов
def update_terminals_hist(con):
    cursor = con.cursor()
    cursor.execute('''
        INSERT INTO DWH_DIM_TERMINALS_HIST (
            terminal_id, terminal_type, terminal_city, terminal_address
        ) SELECT
            terminal_id, terminal_type, terminal_city, terminal_address
        FROM NEW_STG_TERMINALS
    ''')

    cursor.executescript('''
        UPDATE DWH_DIM_TERMINALS_HIST
        SET effective_to = datetime('now', '-1 second')
        WHERE terminal_id IN (SELECT terminal_id FROM CHANGED_STG_TERMINALS)
        AND effective_to = datetime ('2999-12-31 23:59:59');

        INSERT INTO DWH_DIM_TERMINALS_HIST (
            terminal_id, terminal_type, terminal_city, terminal_address
        ) SELECT
            terminal_id, terminal_type, terminal_city, terminal_address
        FROM CHANGED_STG_TERMINALS
    ''')

    cursor.executescript('''
        UPDATE DWH_DIM_TERMINALS_HIST
        SET effective_to = datetime('now', '-1 second')
        WHERE terminal_id IN (SELECT terminal_id FROM DELETED_STG_TERMINALS)
        AND effective_to = datetime ('2999-12-31 23:59:59');

        INSERT INTO DWH_DIM_TERMINALS_HIST (
            terminal_id, terminal_type, terminal_city, terminal_address, deleted_flg
        ) SELECT
            terminal_id, terminal_type, terminal_city, terminal_address, 1
        FROM DELETED_STG_TERMINALS
    ''')

    con.commit()

def drop_stg_tables(con):
    cursor = con.cursor()
    cursor.executescript('''
        DROP TABLE if exists STG_TERMINALS;
        DROP TABLE if exists NEW_STG_TERMINALS;
        DROP TABLE if exists DELETED_STG_TERMINALS;
        DROP TABLE if exists CHANGED_STG_TERMINALS
    ''')

def increment_load(date, con):
    file_path = r'data\terminals_' + date + '.xlsx'
    file_name = 'terminals_' + date + '.xlsx'

    init_terminals_hist(con)
    drop_stg_tables(con)
    excel_load_to_sql(file_path, 'STG_TERMINALS', con)
    create_new_rows(con)
    create_deleted_rows(con)
    create_changed_rows(con)
    update_terminals_hist(con)

    os.chdir('data')
    os.rename(file_name, file_name + '.backup')
    backup_file = file_name + '.backup'
    os.chdir(os.pardir)
    os.replace(r'data\\' + backup_file, r'archive\\' + backup_file)
