import csv
import os
import datetime
import configparser
import mysql.connector

# TODO update letter merge to group by versions
# TODO add support for remove records
# TODO add notes to remove reports


class Global:
    def __init__(self):
        self.database = ''
        self.db_param = {'host': '', 'user': '', 'password': ''}
        self.current_campaign = ''
        self.available_campaigns = []
        self.processing_date = datetime.date.strftime(datetime.date.today(), "%Y-%m-%d")

        self.file_import_header = ['source', 'source_seq', 'unique_id', 'Individual_First_Name_1',
                                   'Individual_Middle_Name_1', 'Individual_Last_Name_1',
                                   'Individual_First_Name_2', 'Individual_Middle_Name_2',
                                   'Individual_Last_Name_2', 'Address_1', 'Address_2', 'County',
                                   'City', 'State', 'ZipCode', 'MSA', 'date_of_birth', 'Individual_1_Phone_number_1',
                                   'Individual_1_Phone_number_2', 'Individual_1_Phone_number_3',
                                   'Individual_2_Phone_number_1', 'Individual_2_Phone_number_2',
                                   'Individual_2_Phone_number_3', 'Individual_1_email_address_1',
                                   'Individual_1_email_address_2', 'Individual_1_email_address_3',
                                   'Individual_2_email_address_1', 'Individual_2_email_address_2',
                                   'Individual_2_email_address_3', 'lead_source', 'Segment',
                                   'Expiration_Date', 'Likely2_Org_Medicare_Score', 'Carrot_Health_Segment',
                                   'mid', 'print_mid', 'media_logic_project_id', 'fbo_caps', 'version',
                                   'art_code', 'orig_county', 'crrt', 'barcode', 'x', 'status_', 'errno_',
                                   'type_', 'lacs_', 'company', 'ocompany', 'oaddress', 'oaddress2',
                                   'ocity', 'ostate', 'ozipcode', 'lot_', 'ascdesc_', 'dp_', 'countyno_',
                                   'stno_', 'lacsind_', 'lacsrc_', 'stelink_', 'zip5', 'dpc', 'latitude_',
                                   'longitude_', 'elatitude', 'elongitude', 'dpv_', 'census_tr', 'census_bl',
                                   'census_rs', 'dpvnotes_', 'vacant_', 'leftout_', 'ffapplied_', 'movetype_',
                                   'movedate_', 'matchflag_', 'nxi_', 'ank_', 'address_group', 'in_service',
                                   'removed', 'm_id', 'std_dmamps', 'std_prison', 'std_deceas', 'desc_dob',
                                   'desc_dod']

        self.file_export_header = ['Wunderman Person ID', 'First Name', 'Last Name', 'Address_1',
                                   'Address_2', 'City', 'State', 'Zip', 'Telephone', 'Email',
                                   'Medica 2D BRC MID', 'Client Person Code', 'Page_IMAGES']

        self.merge_letter_header = ['Campaign', 'Individual_First_Name_1', 'Individual_Last_Name_1',
                                    'Individual_First_Name_2', 'Individual_Last_Name_2', 'Address_1',
                                    'Address_2', 'City', 'State', 'Zip', 'County', 'Unique_ID',
                                    'mid', 'art_code', 'kit'
                                    ]

        self.version_dictionary = {'ML2|Y0088_54602_C': 'OMA', 'ML1|Y0088_54601_C': 'OMA',
                                   'ML3|Y0088_54603_C': 'OMA', 'BRC20 ML7|n/a': 'OMA',
                                   'BRC20 ML8|n/a': 'OMA', 'BRC20 ML9|TCGPTCM': 'TC-TCM',
                                   'BRC20 ML9|TCGPG': 'TC-GTCM', 'BRC20 ML9|TCGPSE': 'TC-SEMN',
                                   'BRC20 ML10|TCWBTCM': 'TC-TCM', 'BRC20 ML10|TCWBG': 'TC-GTCM',
                                   'BRC20 ML10|TCWBSE': 'TC-SEMN', 'BRC20 ML13|OIOMO-LG2': 'OMA',
                                   'FBC20 ML12|OIGP-LG2': 'OMA', 'BRC20 ML14|OIIFP-LG2': 'OMA',
                                   'BRC20 ML15|TCGPTC-LG2': 'TC-TCM', 'BRC20 ML15|TCGPSE-LG2': 'TC-SEMN',
                                   'BRC20 ML16|TCWBTC-LG2': 'TC-TCM', 'BRC20 ML11|TCOMO': 'county',
                                   'BRC20 ML16|TCWBGTSE-LG2': 'county', 'BRC20 ML5|Y0088_54731_C': 'county',
                                   'BRC20 ML4|Y0088_54732_C': 'county'}

        self.special_counties = {'ANOKA': 'TC-TCM', 'CARVER': 'TC-TCM', 'DAKOTA': 'TC-TCM', 'HENNEPIN': 'TC-TCM',
                                 'RAMSEY': 'TC-TCM', 'SCOTT': 'TC-TCM', 'WASHINGTON': 'TC-TCM',
                                 'CHISAGO': 'TC-GTCM', 'ISANTI': 'TC-GTCM', 'STEARNS': 'TC-GTCM',
                                 'KANDIYOHI': 'TC-GTCM', 'WRIGHT': 'TC-GTCM', 'SHERBURNE': 'TC-GTCM',
                                 'BLUE EARTH': 'TC-SEMN', 'BROWN': 'TC-SEMN', 'DODGE': 'TC-SEMN',
                                 'FARIBAULT': 'TC-SEMN', 'FILLMORE': 'TC-SEMN', 'FREEBORN': 'TC-SEMN',
                                 'HOUSTON': 'TC-SEMN', 'MARTIN': 'TC-SEMN', 'MOWER': 'TC-SEMN',
                                 'NICOLLET': 'TC-SEMN', 'OLMSTED': 'TC-SEMN', 'STEELE': 'TC-SEMN',
                                 'WABASHA': 'TC-SEMN', 'WASECA': 'TC-SEMN', 'WATONWAN': 'TC-SEMN',
                                 'WINONA': 'TC-SEMN'}

    def set_current_campaign(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        campaign_dic = dict()
        for n, x in enumerate(g.available_campaigns, 1):
            print("{}: {}".format(n, x))
            campaign_dic[n] = x

        ans = int(input("Set current campaign by number: "))
        while ans not in campaign_dic.keys():
            print("Error: Invalid entry")
            ans = int(input("Set current campaign by number: "))

        self.current_campaign = campaign_dic[ans]
        config.set('db_param', 'campaign', campaign_dic[ans])

        with open('config.ini', 'w') as configfile:
            config.write(configfile)

        print("Current campaign search changed to {0}\n".format(self.current_campaign.upper()))

        main_menu()

    def set_processing_date(self):
        print("\nCurrent processing date is {}".format(g.processing_date))
        ans = input("Enter processing date as YYYY-MM-DD: ")

        try:
            new_date = datetime.datetime.strptime(ans, "%Y-%m-%d").date()
            self.processing_date = new_date
            print("\n** Processing date updated, processing date: {0} **\n\n".format(self.processing_date))
        except ValueError:
            print("\n** Invalid date format, processing date NOT updated **\n\n")

        main_menu()

    def set_db_param(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.db_param['host'] = config['db_param']['host']
        self.db_param['user'] = config['db_param']['user']
        self.db_param['password'] = config['db_param']['password']
        self.database = config['db_param']['database']
        self.current_campaign = config['db_param']['campaign']

    def initialize_folders(self):
        if not os.path.exists('ftp_files'):
            os.mkdir('ftp_files')
        if not os.path.exists('letter_merge'):
            os.mkdir('letter_merge')


def initialize_db():
    sql1 = ("CREATE TABLE `records` ("
            "`campaign` VARCHAR(25) NULL DEFAULT NULL, "
            "`source` VARCHAR(75) NULL DEFAULT NULL, "
            "`source_seq` INT(10) NULL DEFAULT NULL, "
            "`unique_id` VARCHAR(20) NULL DEFAULT NULL, "
            "`Individual_First_Name_1` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_Middle_Name_1` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_Last_Name_1` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_First_Name_2` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_Middle_Name_2` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_Last_Name_2` VARCHAR(100) NULL DEFAULT NULL, "
            "`Address_1` VARCHAR(100) NULL DEFAULT NULL, "
            "`Address_2` VARCHAR(100) NULL DEFAULT NULL, `County` VARCHAR(100) NULL DEFAULT NULL, "
            "`City` VARCHAR(50) NULL DEFAULT NULL, `State` VARCHAR(2) NULL DEFAULT NULL, "
            "`ZipCode` VARCHAR(10) NULL DEFAULT NULL, `MSA` VARCHAR(200) NULL DEFAULT NULL, "
            "`date_of_birth` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_1_Phone_number_1` VARCHAR(25) NULL DEFAULT NULL, "
            "`Individual_1_Phone_number_2` VARCHAR(25) NULL DEFAULT NULL, "
            "`Individual_1_Phone_number_3` VARCHAR(25) NULL DEFAULT NULL, "
            "`Individual_2_Phone_number_1` VARCHAR(25) NULL DEFAULT NULL, "
            "`Individual_2_Phone_number_2` VARCHAR(25) NULL DEFAULT NULL, "
            "`Individual_2_Phone_number_3` VARCHAR(25) NULL DEFAULT NULL, "
            "`Individual_1_email_address_1` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_1_email_address_2` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_1_email_address_3` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_2_email_address_1` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_2_email_address_2` VARCHAR(100) NULL DEFAULT NULL, "
            "`Individual_2_email_address_3` VARCHAR(100) NULL DEFAULT NULL, "
            "`lead_source` VARCHAR(20) NULL DEFAULT NULL, "
            "`Segment` VARCHAR(50) NULL DEFAULT NULL, "
            "`Expiration_Date` VARCHAR(20) NULL DEFAULT NULL, "
            "`Likely2_Org_Medicare_Score` VARCHAR(100) NULL DEFAULT NULL, "
            "`Carrot_Health_Segment` VARCHAR(100) NULL DEFAULT NULL, "
            "`mid` VARCHAR(10) NULL DEFAULT NULL, `print_mid` VARCHAR(10) NULL DEFAULT NULL, "
            "`media_logic_project_id` INT(20) NULL DEFAULT NULL, "
            "`fbo_caps` INT(20) NULL DEFAULT NULL, "
            "`version` VARCHAR(50) NULL DEFAULT NULL, `art_code` VARCHAR(30) NULL DEFAULT NULL, "
            "`orig_county` VARCHAR(100) NULL DEFAULT NULL, `crrt` VARCHAR(4) NULL DEFAULT NULL, "
            "`barcode` VARCHAR(30) NULL DEFAULT NULL, `x` VARCHAR(10) NULL DEFAULT NULL, "
            "`status_` VARCHAR(10) NULL DEFAULT NULL, `errno_` VARCHAR(50) NULL DEFAULT NULL, "
            "`type_` VARCHAR(30) NULL DEFAULT NULL, `lacs_` VARCHAR(30) NULL DEFAULT NULL, "
            "`company` VARCHAR(50) NULL DEFAULT NULL, `ocompany` VARCHAR(50) NULL DEFAULT NULL, "
            "`oaddress` VARCHAR(100) NULL DEFAULT NULL, `oaddress2` VARCHAR(100) NULL DEFAULT NULL, "
            "`ocity` VARCHAR(50) NULL DEFAULT NULL, `ostate` VARCHAR(2) NULL DEFAULT NULL, "
            "`ozipcode` VARCHAR(10) NULL DEFAULT NULL, `lot_` VARCHAR(30) NULL DEFAULT NULL, "
            "`ascdesc_` VARCHAR(30) NULL DEFAULT NULL, `dp_` VARCHAR(10) NULL DEFAULT NULL, "
            "`countyno_` VARCHAR(30) NULL DEFAULT NULL, `stno_` VARCHAR(30) NULL DEFAULT NULL, "
            "`lacsind_` VARCHAR(30) NULL DEFAULT NULL, `lacsrc_` VARCHAR(30) NULL DEFAULT NULL, "
            "`stelink_` VARCHAR(30) NULL DEFAULT NULL, `zip5` VARCHAR(5) NULL DEFAULT NULL, "
            "`dpc` VARCHAR(10) NULL DEFAULT NULL, `latitude_` VARCHAR(20) NULL DEFAULT NULL, "
            "`longitude_` VARCHAR(20) NULL DEFAULT NULL, `elatitude` VARCHAR(20) NULL DEFAULT NULL, "
            "`elongitude` VARCHAR(20) NULL DEFAULT NULL, `dpv_` VARCHAR(30) NULL DEFAULT NULL, "
            "`census_tr` VARCHAR(30) NULL DEFAULT NULL, `census_bl` VARCHAR(30) NULL DEFAULT NULL, "
            "`census_rs` VARCHAR(30) NULL DEFAULT NULL, `dpvnotes_` VARCHAR(30) NULL DEFAULT NULL, "
            "`vacant_` VARCHAR(30) NULL DEFAULT NULL, `leftout_` VARCHAR(30) NULL DEFAULT NULL, "
            "`ffapplied_` VARCHAR(30) NULL DEFAULT NULL, `movetype_` VARCHAR(10) NULL DEFAULT NULL, "
            "`movedate_` VARCHAR(10) NULL DEFAULT NULL, `matchflag_` VARCHAR(10) NULL DEFAULT NULL, "
            "`nxi_` VARCHAR(10) NULL DEFAULT NULL, `ank_` VARCHAR(30) NULL DEFAULT NULL, "
            "`address_group` VARCHAR(30) NULL DEFAULT NULL, "
            "`in_service` VARCHAR(1) NULL DEFAULT '0', `removed` VARCHAR(50) NULL DEFAULT NULL, "
            "`m_id` VARCHAR(10) NULL DEFAULT NULL, `std_dmamps` VARCHAR(1) NULL DEFAULT NULL, "
            "`std_prison` VARCHAR(1) NULL DEFAULT NULL, `std_deceas` VARCHAR(1) NULL DEFAULT NULL, "
            "`desc_dob` VARCHAR(6) NULL DEFAULT NULL, `desc_dod` VARCHAR(8) NULL DEFAULT NULL) "
            "COLLATE='latin1_swedish_ci' ENGINE=InnoDB;")

    sql2 = ("CREATE TABLE `id_entry` ("
            "`unique_id` VARCHAR(25) NULL DEFAULT NULL,"
            "`campaign` VARCHAR(25) NULL DEFAULT NULL,"
            "`entered_email` VARCHAR(100) NULL DEFAULT NULL,"
            "`entered_phone` VARCHAR(100) NULL DEFAULT NULL,"
            "`entry_notes` VARCHAR(200) NULL DEFAULT NULL,"
            "`exported` INT(10) DEFAULT 0, "
            "`log_date` DATE NULL DEFAULT NULL,"
            "`entry_date` DATETIME NULL DEFAULT NULL,"
            "`export_date` DATETIME NULL DEFAULT NULL,"
            "`deceased` INT(1) DEFAULT 0,"
            "`deceased_fname` varchar(100) NULL DEFAULT NULL,"
            "`deceased_lname` varchar(100) NULL DEFAULT NULL,"
            "PRIMARY KEY (`unique_id`, `campaign`)) "
            "COLLATE='latin1_swedish_ci' ENGINE=InnoDB;")

    conn = mysql.connector.connect(database=g.database, **g.db_param)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS `records`;")
    cursor.execute("DROP TABLE IF EXISTS `id_entry`;")
    conn.commit()

    cursor.execute(sql1)
    cursor.execute(sql2)
    conn.commit()

    conn.close()


def execute_sql_script(fle):
    print("Executing script", fle)
    conn = mysql.connector.connect(database=g.database, **g.db_param)
    cursor = conn.cursor()

    f = open(fle, 'r')
    sql_file = f.read()
    f.close()

    sql_commands = sql_file.split(';')

    for n, command in enumerate(sql_commands, 1):
        try:
            cursor.execute(command)
        except mysql.connector.OperationalError as msg:
            print(f"Command skipped (line {n}: {msg}")
            print(f"\t{command}")

    conn.commit()
    conn.close()


def import_records(fle, campaign):
    print("importing: {0}".format(fle))
    import_header = list(g.file_import_header)
    import_header.extend(['campaign'])
    import_header_sql = "`,`".join(import_header)

    conn = mysql.connector.connect(database=g.database, **g.db_param)
    cursor = conn.cursor()

    with open(fle, 'r') as f:
        csvr = csv.DictReader(f, g.file_import_header, delimiter='\t')
        next(csvr)
        for n, line in enumerate(csvr, 1):
            rec_values = ([line[k] for k in g.file_import_header])
            rec_values.extend([campaign])
            rec_values_sql = ('","'.join(map(lambda x: str(x), rec_values)))

            sql = ('INSERT INTO `records` (`{0}`) '
                   'VALUES ("{1}");'.format(import_header_sql, rec_values_sql))
            cursor.execute(sql)
            # if n > 10: break

    conn.commit()
    conn.close()


def show_tables():
    print("Fetching loaded tables")
    conn = mysql.connector.connect(database=g.database, **g.db_param)
    cursor = conn.cursor()

    if not g.available_campaigns:
        sql = "SELECT * FROM records GROUP by `campaign`;"
        cursor.execute(sql)

        results = cursor.fetchall()
        lst = list()

        if len(results) != 0:
            print("Tables loaded for campaigns:")
            for n, result in enumerate(results):
                r = result[0]
                print("\t{0}".format(r))
                lst.append(r)

        else:
            print("Error: No tables loaded")

        g.available_campaigns = lst
        print("\n**Current campaign search for: {0}**\n".format(g.current_campaign))

    else:
        for r in g.available_campaigns:
            print("\t{0}".format(r))
        print("\n**Current campaign search for: {0}**\n".format(g.current_campaign))

    conn.close()


def choose_task():
    ans = input("Choose task\n\t1: start entry\n\t2: export entries\n\t"
                "3: change campaign for entry\n\t4: change processing date\n\t"
                "5: enter deceased records\n\t0: quit: ")
    if ans not in ['1', '2', '3', '4', '5', '0']:
        print("Invalid answer")
        main_menu()

    if ans == '0':
        exit()

    return ans


def write_count_reports(results, datetime_string):
    with open(os.path.join('ftp_files', f'Count Report_{datetime_string}.txt'), 'w+', newline='') as s:
        counts = dict()
        s.write("MID\tCount\n")
        for r in results:
            counts[r[36]] = counts.get(r[36], 0) + 1

        total = 0
        for mid, count in counts.items():
            s.write(f"{mid}\t{count}\n")
            total += count

        s.write("\n{0}\nTotal\t{1}".format(datetime_string[:10], total))


def write_ftp_files(cursor, results, datetime_string):
    with open(os.path.join('ftp_files', f'AbbyyACQ_{datetime_string}.csv'), 'w+', newline='') as s:
        csvw = csv.writer(s, delimiter=',')
        csvw.writerow(g.file_export_header)
        for r in results:
            csvw.writerow([r[3], r[4], r[6], r[10], r[11],
                           r[13], r[14], r[15], r[95], r[94],
                           r[36], '', f'brc_scans_{datetime_string}.pdf', ''])

            sql_update1 = ("UPDATE `id_entry` SET `exported` = (`exported` + 1) WHERE "
                           "(`unique_id` = %s AND `campaign` = %s);")

            sql_update2 = ("UPDATE `id_entry` SET `export_date` = CURRENT_TIMESTAMP WHERE "
                           "(`unique_id` = %s AND `campaign` = %s);")

            cursor.execute(sql_update1, (r[3], r[93]))
            cursor.execute(sql_update2, (r[3], r[93]))


def write_deceased_records(cursor, results, datetime_string):
    deceased_header = ['Unique Person ID', 'First Name', 'Last Name', 'Address 1', 'Address 2',
                       'City', 'State', 'Zip', 'County']

    with open(os.path.join('ftp_files', f'DECEASED_{datetime_string}.csv'), 'w+', newline='') as s:
        csvw = csv.DictWriter(s, fieldnames=deceased_header, delimiter=',')
        csvw.writeheader()

        for r in results:
            w = {'Unique Person ID': r[3],
                 'First Name': r[102],
                 'Last Name': r[103],
                 'Address 1': r[10],
                 'Address 2': r[11],
                 'City': r[13],
                 'State': r[14],
                 'Zip': r[15],
                 'County': r[12]}

            csvw.writerow(w)

            sql_update1 = ("UPDATE `id_entry` SET `exported` = (`exported` + 1) WHERE "
                           "(`unique_id` = %s AND `campaign` = %s);")

            sql_update2 = ("UPDATE `id_entry` SET `export_date` = CURRENT_TIMESTAMP WHERE "
                           "(`unique_id` = %s AND `campaign` = %s);")

            cursor.execute(sql_update1, (r[3], r[93]))
            cursor.execute(sql_update2, (r[3], r[93]))


def write_letter_merge(results, datetime_string):
    with open(os.path.join('letter_merge', f'Letter_MERGE_{datetime_string}.txt'), 'w+', newline='') as s:
        csvw = csv.writer(s, delimiter='\t')
        csvw.writerow(g.merge_letter_header)
        for r in results:
            kit_lookup = g.version_dictionary["{}|{}".format(r[36], r[40])]

            if kit_lookup == 'county':
                kit_code = g.special_counties[r[12].upper()]
            else:
                kit_code = kit_lookup

            csvw.writerow([g.current_campaign, r[4], r[6], r[7], r[9], r[11], r[10],
                           r[13], r[14], r[15], r[12], r[3], r[36],
                           r[40], kit_code])


def export_results():
    """ all results for day, all unexported results for day
    """
    conn = mysql.connector.connect(database=g.database, **g.db_param)
    cursor = conn.cursor()

    print("\nExporting results")
    ans = input("\nChoose export type\n\t1: Export for ALL not previously exported"
                "\n\t2: Export ALL for today\n\t3: Export ALL for date"
                "\n\t4: Export not previously exported for date\n\t5: Export not previously exported deceased"
                "\n\t0: exit to main menu\n: ")

    while ans not in ['0', '1', '2', '3', '4', '5']:
        print("Invalid answer")
        ans = input("\nChoose export type\n\t1: Export for ALL not previously exported"
                    "\n\t2: Export ALL for unique ID entered TODAY\n\t3: Export ALL for date"
                    "\n\t4: Export not previously exported for date\n\t5: Export not previously exported deceased"
                    "\n\t0: exit to main menu\n: ")

    if ans == '0':
        main_menu()

    if ans == '5':
        sql = ("SELECT a.*, b.* FROM `records` AS a "
               "JOIN id_entry as b "
               "ON a.unique_id = b.unique_id AND a.campaign = b.campaign "
               "WHERE (b.exported = 0 AND b.deceased = 1) "
               "ORDER BY a.`campaign`, a.`art_code`;")

        cursor.execute(sql)
        results = cursor.fetchall()

        datetime_string = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%H-%M-%S")
        write_deceased_records(cursor, results, datetime_string)
        conn.commit()

    if ans == '1':
        cursor.execute("SELECT DATE(log_date) FROM `id_entry` WHERE exported = 0 AND `deceased` = 0 "
                       "GROUP BY DATE(log_date);")
        dates = cursor.fetchall()

        for d in dates:
            sql = ("SELECT a.*, b.* FROM `records` AS a "
                   "JOIN id_entry as b "
                   "ON a.unique_id = b.unique_id AND a.campaign = b.campaign "
                   "WHERE DATE(b.log_date) = %s AND b.exported = 0 "
                   "ORDER BY a.`campaign`, a.`art_code`;")

            d_parts = str.split(str(d[0]), '-')
            cursor.execute(sql, d)
            results = cursor.fetchall()

            save_date_string = datetime.datetime(year=int(d_parts[0]),
                                                 month=int(d_parts[1]),
                                                 day=int(d_parts[2]),
                                                 hour=datetime.datetime.now().hour,
                                                 minute=datetime.datetime.now().minute,
                                                 second=datetime.datetime.now().second)
            datetime_string = datetime.datetime.strftime(save_date_string, "%Y-%m-%d_%H-%M-%S")

            write_ftp_files(cursor, results, datetime_string)
            conn.commit()
            write_letter_merge(results, datetime_string)
            write_count_reports(results, datetime_string)

    if ans == '2':
        sql = ("SELECT a.*, b.* FROM `records` AS a "
               "JOIN id_entry as b "
               "ON a.unique_id = b.unique_id AND a.campaign = b.campaign "
               "WHERE DATE(b.log_date) = CURDATE() AND b.`deceased` = 0 "
               "ORDER BY a.`campaign`, a.`art_code`;")

    if ans == '3':
        export_date = input("Export ALL for date (YYYY-MM-DD): ")
        sql = ("SELECT a.*, b.* FROM `records` AS a "
               "JOIN id_entry as b "
               "ON a.unique_id = b.unique_id AND a.campaign = b.campaign "
               "WHERE DATE(b.log_date) = '{0}' AND b.`deceased` = 0 "
               "ORDER BY a.`campaign`, a.`art_code`;".format(export_date))

    if ans == '4':
        export_date = input("Export not previously exported for date (YYYY-MM-DD): ")
        sql = ("SELECT a.*, b.* FROM `records` AS a "
               "JOIN id_entry as b "
               "ON a.unique_id = b.unique_id AND a.campaign = b.campaign "
               "WHERE DATE(b.log_date) = '{0}' AND b.exported = 0 AND b.`deceased` = 0 "
               "ORDER BY a.`campaign`, a.`art_code`;".format(export_date))

    if ans in ['2', '3', '4']:
        # print(sql)
        cursor.execute(sql)
        results = cursor.fetchall()

        datetime_string = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%H-%M-%S")
        if ans in ['3', '4']:
            datetime_string = datetime.datetime.strftime(datetime.datetime.now(), "{0}_%H-%M-%S".format(export_date))

        write_ftp_files(cursor, results, datetime_string)
        conn.commit()
        write_letter_merge(results, datetime_string)
        write_count_reports(results, datetime_string)

    conn.close()

    export_results()


def deceased_entry():
    """
        0 to exit to start processing menu
        enter MID
        display matching results
        set aside if not matching
    """
    conn = mysql.connector.connect(database=g.database, **g.db_param)
    cursor = conn.cursor()

    print("\nenter '0' to exit to main menu")
    print("\nID will search in campaign, {0}".format(g.current_campaign))
    ans = input("\nEnter unique id as deceased ({0}): ".format(g.current_campaign))
    error = False

    while ans != '0':
        sql = ("SELECT a.*, b.* FROM `records` AS a "
               "LEFT JOIN id_entry as b "
               "ON a.unique_id = b.unique_id AND a.campaign = b.campaign "
               "WHERE a.`unique_id` = %s AND a.`campaign` = %s;")

        # print(sql, (ans, g.current_campaign,))
        cursor.execute(sql, (ans, g.current_campaign))

        results = cursor.fetchall()
        if len(results) == 0:
            print("No result found")
            error = True
        if len(results) > 1:
            print("Error: unique id returns more than one result.\n:"
                  "See list administrator")
            error = True

        if not error:
            for result in results:
                # print(result)
                print("Search result:")
                print("Unique ID: {0}\nMID: {1}".format(result[3], result[36]))
                print("Name 1: {0} {1}".format(result[4], result[6]))
                if result[7] != '':
                    print("Name 2: {0} {1}".format(result[7], result[9]))
                print("Address: {0}".format(result[10]))
                if result[11] != '':
                    print("         : {0}".format(result[11]))
                print("{0}, {1} {2}".format(result[13], result[14], result[15]))

                if result[98] is not None:
                    ans = input("\n** Unique ID previously "
                                "logged on {0}, REPLACE? "
                                "(yes: 1 / no: 0): ".format(result[99]))
                    if ans != '1':
                        error = True

            if not error:
                ans = input("Mark as deceased? (yes: 1 / no: 0): ")
                while ans not in ['1', '0']:
                    print("Invalid response")
                    ans = input("Mark as deceased? (yes: 1 / no: 0): ")

                if ans == '1':
                    first_name = input("deceased first name: ").strip()
                    last_name = input("deceased last name: ").strip()
                    notes = input("additional notes: ").strip()

                    sql = ("REPLACE INTO `id_entry` (`unique_id`, `log_date`, `deceased`, "
                           "`entry_notes`, `campaign`, `exported`, `entry_date`, `deceased_fname`,"
                           "`deceased_lname`) "
                           "VALUES (%s, %s, %s, %s, %s, 0, CURRENT_TIMESTAMP, %s, %s);")

                    cursor.execute(sql, (result[3], g.processing_date, 1,
                                         notes, g.current_campaign, first_name, last_name))
                    conn.commit()

                else:
                    print("Entry not recorded")

        error = False
        ans = input("\nEnter unique id as deceased ({0}): ".format(g.current_campaign.upper()))

    conn.close()
    main_menu()


def unique_id_entry():
    """
        0 to exit to start processing menu
        enter MID
        display matching results
        set aside if not matching
    """
    conn = mysql.connector.connect(database=g.database, **g.db_param)
    cursor = conn.cursor()

    print("\nenter '0' to exit to main menu")
    print("\nID will search in campaign, {0}".format(g.current_campaign))
    ans = input("\nEnter unique id ({0}): ".format(g.current_campaign))
    error = False

    while ans != '0':
        sql = ("SELECT a.*, b.* FROM `records` AS a "
               "LEFT JOIN id_entry as b "
               "ON a.unique_id = b.unique_id AND a.campaign = b.campaign "
               "WHERE a.`unique_id` = %s AND a.`campaign` = %s;")

        # print(sql, (ans, g.current_campaign,))
        cursor.execute(sql, (ans, g.current_campaign))

        results = cursor.fetchall()
        if len(results) == 0:
            print("No result found")
            error = True
        if len(results) > 1:
            print("Error: unique id returns more than one result.\n:"
                  "See list administrator")
            error = True

        if not error:
            for result in results:
                # print(result)
                print("Search result:")
                print("Unique ID: {0}\nMID: {1}".format(result[3], result[36]))
                print("Name 1: {0} {1}".format(result[4], result[6]))
                if result[7] != '':
                    print("Name 2: {0} {1}".format(result[7], result[9]))
                print("Address: {0}".format(result[10]))
                if result[11] != '':
                    print("         : {0}".format(result[11]))
                print("{0}, {1} {2}".format(result[13], result[14], result[15]))

                if result[98] is not None:
                    ans = input("\n** Unique ID previously "
                                "logged on {0}, REPLACE? "
                                "(yes: 1 / no: 0): ".format(result[99]))
                    if ans != '1':
                        error = True

            if not error:
                ans = input("Accept entry? (yes: 1 / no: 0): ")
                while ans not in ['1', '0']:
                    print("Invalid response")
                    ans = input("Accept entry? (yes: 1 / no: 0): ")

                if ans == '1':
                    email = input("provided email: ").strip()
                    phone = input("provided phone (numbers only): ").strip()
                    notes = input("additional notes: ").strip()

                    phone = "".join(filter(lambda x: x.isdigit(), phone))

                    sql = ("REPLACE INTO `id_entry` (`unique_id`, `log_date`, `entered_email`,"
                           "`entered_phone`, `entry_notes`, `campaign`, `exported`, `entry_date`) "
                           "VALUES (%s, %s, %s, %s, %s, %s, 0, CURRENT_TIMESTAMP);")

                    cursor.execute(sql, (result[3], g.processing_date, email, phone,
                                         notes, g.current_campaign))
                    conn.commit()

                else:
                    print("Entry not recorded")

        error = False
        ans = input("\nEnter unique id ({0}): ".format(g.current_campaign.upper()))

    conn.close()
    main_menu()


def main_menu(display_tables=True):
    if display_tables:
        show_tables()

    ans = choose_task()

    if ans == '5':
        deceased_entry()

    if ans == '4':
        g.set_processing_date()

    if ans == '3':
        g.set_current_campaign()

    if ans == '2':
        export_results()

    if ans == '1':
        unique_id_entry()


def main():
    global g
    g = Global()
    g.initialize_folders()
    g.set_db_param()
    # initialize_db()
    # import_records(os.path.join('records', 'full_list_lg1.txt'), 'LG1')
    # import_records(os.path.join('records', 'full_list_lg2.txt'), 'LG2')
    # import_records(os.path.join('records', 'full_list_preheat.txt'), 'Preheat')
    # execute_sql_script('code_corrections.sql')
    main_menu()


if __name__ == '__main__':
    main()
