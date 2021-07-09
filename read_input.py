import  os
from pathlib import Path

def getFilePath():
    #get current path
    script_path = os.path.abspath(os.path.dirname(__file__)) 
    # get txt folder path
    txt_folder_path = os.path.join(script_path,'chats')
    # set path
    txt_dir = Path(txt_folder_path)
    return txt_dir

def read_file():
    # get files with is text file
    txt_files = getFilePath().glob('*.txt')
    connect_db()
    for txt_file in txt_files:
        file=open(txt_file,'r')
        print(file.readlines())
        #TODO 전처리 해서 이 부분이 정당한 텍스트인지 확인 해야함
        # 해당 경우 아니면 넘어가버려
        # 1. 요일, 월 일, 년도 (e.g., Saturday, January 11, 2020)
        #     ->월과 년도 만 파싱해서 테이블 있는지 확인
        #         -> 없으면 생성
        # 2. 월 일, 연도 시:분, 사용자 : 내용 (e.g., Jan 11, 2020 22:14, 김XX : 여행 가고싶다)
        #     해당 날짜가 
        #     -> 각 부분 파싱하여 위 테이블에 추가
        # break

def is_date_format():
    pass
def is_msg_format():
    pass

def connect_db():
    pass

def close_db():
    pass

def insert_db(datetime, sender, receiver, content):
    pass



def main():
    pass

if __name__ == '__main__':
    read_file()
    pass
