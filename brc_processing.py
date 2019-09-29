import csv
import os
import sqlite3


class Global:
    def __init__(self):
        self.database = 'medica_brc_entry.db'
        self.current_campaign = ''
        self.available_campaigns = []

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

    def set_current_campaign(self):
        campaign_dic = dict()
        for n, x in enumerate(g.available_campaigns, 1):
            print("{}: {}".format(n, x))
            campaign_dic[n] = x

        ans = int(input("Set current campaign by number: "))
        while ans not in campaign_dic.keys():
            print("Error: Invalid entry")
            ans = int(input("Set current campaign by number: "))

        self.current_campaign = campaign_dic[ans]
        print("Current campaign search changed to {0}\n".format(self.current_campaign.upper()))

        main_menu()


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
            "`desc_dob` VARCHAR(6) NULL DEFAULT NULL, `desc_dod` VARCHAR(8) NULL DEFAULT NULL);")

    sql2 = ("CREATE TABLE `id_entry` ("
            "`unique_id` VARCHAR(25) NULL DEFAULT NULL,"
            "`exported` INT(10) DEFAULT 0, "
            "`logdate` DATETIME NULL DEFAULT NULL);")

    conn = sqlite3.connect(database=g.database)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS `records`;")
    cursor.execute("DROP TABLE IF EXISTS `id_entry`;")
    conn.commit()

    cursor.execute(sql1)
    cursor.execute(sql2)
    conn.commit()

    cursor.execute("VACUUM;")
    conn.close()


def import_records(fle, campaign):

    print("importing: {0}".format(fle))
    import_header = list(g.file_import_header)
    import_header.extend(['campaign'])
    import_header_sql = "`,`".join(import_header)

    conn = sqlite3.connect(database=g.database)
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
    conn = sqlite3.connect(database=g.database)
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
                if n == 0:
                    g.current_campaign = r
        else:
            print("Error: No tables loaded")

        g.available_campaigns = lst
        print("\n**Current campaign search for: {0}**\n".format(g.current_campaign))

    else:
        for r in g.available_campaigns:
            print("\t{0}".format(r))

    conn.close()


def choose_task():
    ans = input("Choose task (1: start entry, 2: export entries, 3: change campaign for entry, 0: quit): ")
    if ans not in ['1', '2', '3', '0']:
        print("Invalid answer")
        main_menu()

    if ans == '0':
        exit()

    return ans


def export_results():
    """ all results for day, all unexported results for day
    """
    pass
    main_menu()


def unique_id_entry():
    """
        0 to exit to start processing menu
        enter MID, reject on ML6
        display matching results
        set aside if not matching
    """
    conn = sqlite3.connect(database=g.database)
    cursor = conn.cursor()

    print("\nenter '0' to exit to main menu")
    print("\nID will search in campaign, {0}".format(g.current_campaign))
    ans = input("\nEnter unique id ({0}): ".format(g.current_campaign))

    while ans != '0':
        sql = "SELECT * FROM `records` WHERE `unique_id` = ? AND `campaign` = ?;"
        cursor.execute(sql, (ans, g.current_campaign))

        results = cursor.fetchall()
        if len(results) == 0:
            print("No result found")
        if len(results) > 1:
            print("Error: unique id returns more than one result.\n:"
                  "See list administrator")

        for result in results:
            print("Search result:")
            print("Unique ID: {0} MID: {1}".format(result[3], result[36]))
            print("Name 1: {0} {1}".format(result[4], result[6]))
            if result[7] != '':
                print("Name 2: {0} {1}".format(result[7], result[9]))
            print("Address: {0}".format(result[10]))
            if result[11] != '':
                print("         : {0}".format(result[11]))
            print("{0}, {1} {2}".format(result[12], result[14], result[15]))

        ans = input("\nEnter unique id ({0}): ".format(g.current_campaign.upper()))

    conn.close()
    main_menu()


def main_menu(display_tables=True):
    if display_tables:
        show_tables()

    ans = choose_task()

    if ans == '3':
        g.set_current_campaign()

    if ans == '2':
        # export results
        export_results()

    if ans == '1':
        # start id entry
        unique_id_entry()


def main():
    global g
    g = Global()
    # initialize_db()
    # import_records(os.path.join('records', 'full_list_lg1.txt'), 'LG1')
    # import_records(os.path.join('records', 'full_list_lg2.txt'), 'LG2')
    # import_records(os.path.join('records', 'full_list_preheat.txt'), 'Preheat')
    main_menu()


if __name__ == '__main__':
    main()
