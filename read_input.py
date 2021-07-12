import os
from pathlib import Path
import pymysql
import re
from datetime import datetime

DATABASE_NAME = "chats"
HOST_NAME = "localhost"
USER_ID = "root"
USER_PASSWORD = "rootPW1!"
CHARSET = "utf8"
TXT_FOLDER_PATH = "chats"
MIN_DATE = 1
MAX_DATE = 31
MIN_HOUR = 0
MAX_HOUR = 23
MIN_MINUTE = 0
MAX_MINUTE = 59
MIN_YEAR = 2000
MAX_YEAR = 3000

daynames = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

months_short = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}
months = {
    "January": "01",
    "Feburary": "02",
    "March": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}

# set users name
USER_DICT = {"김대엽": "A", "(Unknown)": "B", "꩓𝑱𝒊𝒏𝒏𝒚꩓": "B"}


# read file and create/insert into database
def read_file():
    # get files which is text file
    txt_files = getFilePath().glob("*.txt")
    conn = get_connection()
    create_and_connect_db(conn, DATABASE_NAME)
    create_idx_table(conn)

    total = getFileCounts(getFilePath())
    current = 0
    # print(file_count)
    for txt_file in txt_files:
        current += 1
        print(".......................")
        print("Reading file:" + str(txt_file) + "------>", end="")

        with open(txt_file) as lines:
            has_read_message = False
            tn, t, s, m = "", "", "", ""
            for line in lines:
                # print(line)
                if is_msg_format(line):
                    year, month = get_year_month_by_msg(line)
                    # if new year, month -> create new table
                    create_monthly_chat_table(conn, year, month)
                    # already read a message format and met a new message -> insert
                    if has_read_message:
                        insert_into_monthly_chat_table(conn, tn, t, s, m)
                    # check status that message is read (could be appened)
                    has_read_message = True
                    tn, t, s, m = get_tablename_time_sender_message(line)
                elif is_date_format(line):
                    year, month = get_year_month_by_date(line)
                    # if new year, month -> create new table
                    create_monthly_chat_table(conn, year, month)
                    # already read a message and met a new date format -> insert
                    if has_read_message:
                        insert_into_monthly_chat_table(conn, tn, t, s, m)
                        has_read_message = False
                # for appending messages, file end, hangle empty space
                else:
                    if len(line) > 0:
                        m += " " + str(line.strip("\n"))
            # isert last message when file ends
            insert_into_monthly_chat_table(conn, tn, t, s, m)
        print("Done!(%d/%d)" % (current, total))
    print(".......................")
    print("All files inserted!")
    close_db(conn)


# check if line is a message format
def is_msg_format(line):
    tokens = [token.strip(" ") for token in line.split(",", 2)]
    if len(tokens) == 3:
        try:
            month_day = tokens[0].split()
            year_time = tokens[1].split()
            hour_minute = year_time[1].split(":")
            if (
                month_day[0] in months_short
                and is_day(month_day[1])
                and is_year(year_time[0])
                and is_hour(hour_minute[0])
                and is_minute(hour_minute[1])
            ):
                return True
        except:
            return False
    else:
        return False


# check if line is a date format
def is_date_format(line):
    tokens = [token.strip(",") for token in line.split(" ")]
    if (
        len(tokens) == 4
        and tokens[0] in daynames
        and tokens[1] in months
        and is_day(tokens[2])
        and is_year(tokens[3].strip("\n"))
    ):
        return True
    else:
        return False


# check if string is year format
def is_year(str):
    if str.isdecimal() and int(str) >= MIN_YEAR and int(str) <= MAX_YEAR:
        return True
    else:
        return False


# check if string is day format
def is_day(str):
    if str.isdecimal() and int(str) >= MIN_DATE and int(str) <= MAX_DATE:
        return True
    else:
        return False


# check if string is hour format
def is_hour(str):
    if str.isdecimal() and int(str) >= MIN_HOUR and int(str) <= MAX_HOUR:
        return True
    else:
        return False


# check if string is minute format
def is_minute(str):
    if str.isdecimal() and int(str) >= MIN_MINUTE and int(str) <= MAX_MINUTE:
        return True
    else:
        return False


# get year and month string from msg line
def get_year_month_by_msg(line):
    tokens = [token.strip(",") for token in line.split(",", 2)]
    month_day = tokens[0].split()
    year_time = tokens[1].split()
    return year_time[0], months_short.get(month_day[0])


# get year and month string from date line
def get_year_month_by_date(line):
    tokens = [token.strip(",") for token in line.split(" ")]
    return tokens[3].strip("\n"), months[tokens[1]]


# get tablename, datetome, sender and message from the line
def get_tablename_time_sender_message(line):
    tokens = [token.strip(",") for token in line.split(",", 2)]
    month_day = tokens[0].split()
    year_time = tokens[1].split()
    hour_minute = year_time[1].split(":")
    sender_message = [token.strip() for token in tokens[2].split(":")]
    date_time_str = "%s-%s-%s %s:%s:00" % (
        year_time[0],
        months_short.get(month_day[0]),
        month_day[1],
        hour_minute[0],
        hour_minute[1],
    )
    date_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
    return (
        year_time[0] + months_short.get(month_day[0]) + "chats",
        date_time,
        USER_DICT.get(sender_message[0]),
        sender_message[1],
    )


# get connection with the local database
def get_connection():
    conn = pymysql.connect(
        host=HOST_NAME, user=USER_ID, passwd=USER_PASSWORD, charset=CHARSET
    )
    return conn


# returns files directory
def getFilePath():
    script_path = os.path.abspath(os.path.dirname(__file__))
    txt_folder_path = os.path.join(script_path, TXT_FOLDER_PATH)
    txt_dir = Path(txt_folder_path)
    return txt_dir


def getFileCounts(path):
    dirListing = os.listdir(path)
    return len(dirListing)


def create_and_connect_db(conn, dbname):
    sql = "CREATE DATABASE IF NOT EXISTS " + dbname + ";"
    conn.cursor().execute(sql)
    sql = "USE " + dbname + ";"
    conn.cursor().execute(sql)
    conn.commit()


def create_idx_table(conn):
    sql = """CREATE TABLE IF NOT EXISTS indexes (
        year SMALLINT NOT NULL,
        month TINYINT NOT NULL,
        PRIMARY KEY(year,month)
    )
    """
    conn.cursor().execute(sql)
    conn.commit()


def create_monthly_chat_table(conn, year, month):
    table_name = year + month + "chats"
    sql = (
        """CREATE TABLE IF NOT EXISTS `%s` (
  `date` datetime NOT NULL,
  `sender` varchar(20) NOT NULL,
  `message` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`date`,`sender`,`message`))
    """
        % table_name
    )
    conn.cursor().execute(sql)
    conn.commit()
    insert_into_index_table(conn, year, month)


def insert_into_index_table(conn, year, month):
    sql = "INSERT IGNORE INTO indexes(year, month) VALUES (%s, %s);" % (
        year,
        month,
    )
    conn.cursor().execute(sql)
    conn.commit()


def insert_into_monthly_chat_table(conn, table_name, datetime, sender, message):
    # message = message.strip()
    # print(message, end=" ")
    message = clean_text(message)

    sql = "INSERT IGNORE INTO %s(date, sender, message) VALUES ('%s', '%s', '%s');" % (
        table_name,
        datetime,
        sender,
        message,
    )
    conn.cursor().execute(sql)
    conn.commit()


def remove_emoji(string):
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub(r"", string)


def clean_text(str):
    res = re.sub("[-=+,#/\?:^$.@*\"※~&%ㆍ!』\‘|\(\)\[\]\<\>`'…》]", "", str).replace(
        "\\", ""
    )
    res = remove_emoji(res)
    return res


def close_db(conn):
    conn.cursor().close()


if __name__ == "__main__":
    read_file()
    print("Program ends!")
