import os
from pathlib import Path
import pymysql

DATABASE_NAME = "testdb"
HOST_NAME = "localhost"
USER_ID = "root"
USER_PASSWORD = "rootPW1!"
CHARSET = "utf8"
TXT_FOLDER_PATH = "test"
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

USER_DICT = {"김대엽": "A", "(Unknown)": "B", "꩓𝑱𝒊𝒏𝒏𝒚꩓": "B"}


def getFilePath():
    # get current path
    script_path = os.path.abspath(os.path.dirname(__file__))
    # get txt folder path
    txt_folder_path = os.path.join(script_path, TXT_FOLDER_PATH)
    # set path
    txt_dir = Path(txt_folder_path)
    return txt_dir


def read_file():
    # get files with is text file
    txt_files = getFilePath().glob("*.txt")
    conn = get_connection()
    create_and_connect_db(conn, DATABASE_NAME)
    create_idx_table(conn)
    for txt_file in txt_files:
        with open(txt_file) as lines:
            is_msg_appending = False
            appending_msg = ""
            # sender,
            for line in lines:
                if is_msg_format(line):

                    # if not is_msg_appending :
                    print("MESSGAE FORMAT")
                    print(line)
                    t, s, m = get_time_sender_message(line)
                    # t, s, r, m = get_time_sender_reciver_message()
                    # is_msg_appending = False
                    # # 1. 일단 읽어
                    # # 2. 다음 꺼(공백이면 패스)가 똑같이 메세지 포맷이거나 날짜 포맷이면 읽은 것들 삽입
                    # # 3. 다음 꺼(공백이면 패스)가 메세지 포맷이 아니거나 읽은 메세지에서 어펜드한다
                    # print("aaaaaa")
                    # print(line)
                    pass
                elif is_date_format(line):
                    y, m = get_year_month(line)
                    create_monthly_table(conn, y, m)
                else:
                    pass

        # TODO 전처리 해서 이 부분이 정당한 텍스트인지 확인 해야함
        # 해당 경우 아니면 넘어가버려
        # 1. 요일, 월 일, 년도 (e.g., Saturday, January 11, 2020)
        #     ->월과 년도 만 파싱해서 테이블 있는지 확인
        #         -> 없으면 생성
        # 2. 월 일, 연도 시:분, 사용자 : 내용 (e.g., Jan 11, 2020 22:14, 김XX : 여행 가고싶다)
        #     해당 날짜가
        #     -> 각 부분 파싱하여 위 테이블에 추가
        # break


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


def get_time_sender_message(line):
    tokens = [token.strip(",") for token in line.split(",", 2)]
    month_day = tokens[0].split()
    year_time = tokens[1].split()
    hour_minute = year_time[1].split(":")
    sender_message = [token.strip() for token in tokens[2].split(":")]
    print(sender)
    return (
        (
            "%s-%s-%s %s:%s:00"
            % (year_time[0], month_day[0], month_day[1], hour_minute[0], hour_minute[1])
        ),
        sender_message[0],
        sender_message[1],
    )

    # tokens[3].strip("\n"), months[tokens[1]]


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


def get_year_month(line):
    tokens = [token.strip(",") for token in line.split(" ")]
    return tokens[3].strip("\n"), months[tokens[1]]


def is_year(str):
    if str.isdecimal() and int(str) >= MIN_YEAR and int(str) <= MAX_YEAR:
        return True
    else:
        return False


def is_day(str):
    if str.isdecimal() and int(str) >= MIN_DATE and int(str) <= MAX_DATE:
        return True
    else:
        return False


def is_hour(str):
    if str.isdecimal() and int(str) >= MIN_HOUR and int(str) <= MAX_HOUR:
        return True
    else:
        return False


def is_minute(str):
    if str.isdecimal() and int(str) >= MIN_MINUTE and int(str) <= MAX_MINUTE:
        return True
    else:
        return False


def get_connection():
    conn = pymysql.connect(
        host=HOST_NAME, user=USER_ID, passwd=USER_PASSWORD, charset=CHARSET
    )
    return conn


def create_and_connect_db(conn, dbname):
    sql = "CREATE DATABASE IF NOT EXISTS " + dbname + ";"
    conn.cursor().execute(sql)
    sql = "USE " + dbname + ";"
    conn.cursor().execute(sql)
    conn.commit()


def create_idx_table(conn):
    sql = """CREATE TABLE IF NOT EXISTS indexes (
        table_name MEDIUMINT PRIMARY KEY,
        year SMALLINT,
        month TINYINT
    )
    """
    conn.cursor().execute(sql)
    conn.commit()


def create_monthly_table(conn, year, month):
    table_name = year + month + ""
    sql = (
        """CREATE TABLE IF NOT EXISTS `%s` (
  `message_id` int NOT NULL AUTO_INCREMENT,
  `date` datetime DEFAULT NULL,
  `sender` varchar(20) DEFAULT NULL,
  `message` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`message_id`))
    """
        % table_name
    )
    conn.cursor().execute(sql)
    conn.commit()
    # print("CREATED")
    insert_into_index_table(conn, year, month)


def insert_into_index_table(conn, year, month):
    sql = "INSERT IGNORE INTO indexes(table_name, year, month) VALUES (%s, %s, %s);" % (
        year + month,
        year,
        month,
    )
    # print(sql)
    conn.cursor().execute(sql)
    conn.commit()


def close_db():
    pass


def insert_db(datetime, sender, content):
    pass


if __name__ == "__main__":
    read_file()
    pass
