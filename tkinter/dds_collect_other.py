import threading
import tkinter as tk
from tkinter import *
from tkinter import filedialog
from PIL import ImageTk, Image
import time
from time import sleep
import datetime
import queue
import schedule
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

from collect import collect_dust
from collect import collect_vssl
from collect import collect_wind
from tcp import TcpSocket

temp_day = time.localtime().tm_mday
temp_h = time.localtime().tm_hour
collection_delay = 1
check_min = 50
quit_value = 1
filename = ""
isten = False
isFirst = False

isWind = False
isDust = False
isVssl = False

"""
프로그램 실행시 바로 수집 시작.
메인 : Application 실행
ThreadTest : 보조 스레드, csv파일 수집.

수집
- 첫실행
- 매일 12시(정오, 자정)

global collection_delay
3600 = 1시간
60 = 1분
"""


def nowtimeHMf():
    # tk Frame의 Text에 출력할 시간을 출력하는 함수.
    # 입력 : 없.
    # 출력 : 년-월-일 시:분:.ms3자리
    now = datetime.datetime.now()
    sttime = now.strftime('%Y-%m-%d %H:%M:%S')
    msec = now.strftime('%f')
    return sttime + "." + msec[:3]


class ThreadTest(threading.Thread):
    # tk Frame(Application)에서 실행하는 보조 스레드
    # 메인 스레드에서 종료시키기 전까지 무한루프로 보조스레드에서 계속 돌아감.
    # 첫 실행, 매일 12시(정오,자정)에 CSV파일 수집.

    def __init__(self, app, q):
        threading.Thread.__init__(self)
        self.q = q
        self.app = app

    def run(self):
        global temp_day
        global temp_h
        global collection_delay
        global quit_value
        global isten

        app.startButton.config(state=tk.DISABLED)

        # 처음 실행시 수집
        # self.callwfd(self.app)

        if isFirst:
            self.call_1st(self.app)
        else:
            self.callwfd(self.app)

        while quit_value:
            sleep(collection_delay)
            if datetime.now().strftime('%M:%S') == "50:10":
                self.callwfd(self.app)

    def call_1st(self, app):
        app.text.insert(tk.END, "----------start----------\n")
        if isWind:
            app.text.insert(tk.END, f"wind_data_first_start  =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
            # get_wind_1st()
            app.text.insert(tk.END, f"first_wind_data_end    =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
        else:
            app.text.insert(tk.END, f"if you want to collect Wind data, check Wind checkbox\n")
        if isDust:
            app.text.insert(tk.END, f"dust_data_first_start  =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
            # get_dust_1st()
            app.text.insert(tk.END, f"first_dust_data_end    =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
        else:
            app.text.insert(tk.END, f"if you want to collect Dust data, check Dust checkbox\n")
        if isVssl:
            app.text.insert(tk.END, f"vssl_data_first_start  =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
            # get_vssl_with_check_1st()
            app.text.insert(tk.END, f"first_vssl_data_end    =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
        else:
            app.text.insert(tk.END, f"if you want to collect Vssl data, check Vssl checkbox\n")
        app.text.insert(tk.END, "----------end----------\n")
        app.t2.room.send_all_clients("adf")
        print("thread test is running")

    # CSV 파일 수집 모듈을 호출하는 함수
    def callwfd(self, app):
        app.text.insert(tk.END, "----------start----------\n")
        if isWind:
            app.text.insert(tk.END, f"wind_data_start  =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
            collect_wind.get_wind()
            app.text.insert(tk.END, f"wind_data_end    =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
        else:
            app.text.insert(tk.END, f"if you want to collect Wind data, check Wind checkbox\n")
        if isDust:
            app.text.insert(tk.END, f"dust_data_start  =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
            collect_dust.get_dust()
            app.text.insert(tk.END, f"dust_data_end    =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
        else:
            app.text.insert(tk.END, f"if you want to collect Dust data, check Dust checkbox\n")
        if isVssl:
            app.text.insert(tk.END, f"vssl_data_start    =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
            collect_vssl.get_vssl_with_check_1st()
            app.text.insert(tk.END, f"vssl_data_end    =======  {datetime.now().strftime('%m/%d  %H:%M:%S')}\n")
        else:
            app.text.insert(tk.END, f"if you want to collect Vssl data, check Vssl checkbox\n")
        app.text.insert(tk.END, "----------end----------\n")
        app.t2.room.send_all_clients(f"{datetime.now().strftime('%Y-%m-%d %H:%M')} collect complete")
        print("thread test is running")


'''
메인스레드
메뉴바 (file(save, saveas, quit))
Text
하단배너.
'''


class Application(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.pack()
        self.create_widgets()
        self.q = queue.Queue(10)

        self.t2 = TcpSocket.ServerMain(self, self.q)
        # window 창 크기 고정
        root.resizable(0, 0)



    def create_widgets(self):
        root.title("dds 데이터 수집 모듈")

        self.startButton = tk.Button()
        self.startButton.config(width=5, text="START", command=self.t_test)
        self.startButton.pack(side="bottom")

        self.tcpButton = tk.Button()
        self.tcpButton.config(width=5, text="TCP", command=self.tcp_thread)
        self.tcpButton.pack(side="bottom")

        # check first
        boolCheckBtn = tk.BooleanVar()
        def check_button_command():
            global isFirst
            isFirst = boolCheckBtn.get()
        self.checkButton = tk.Checkbutton(
            variable=boolCheckBtn, onvalue=True, offvalue=False, command=check_button_command, text="startWithFirst")
        self.checkButton.pack(side="bottom")

        # check Wind
        boolCheckBtnWind = tk.BooleanVar()
        def check_wind_command():
            global isWind
            isWind = boolCheckBtnWind.get()
        self.checkWind = tk.Checkbutton(variable=boolCheckBtnWind, onvalue=True, offvalue=False,
                                          command=check_wind_command, text="Wind")
        self.checkWind.pack(side=LEFT, padx=(200, 2))

        # check Dust
        boolCheckBtnDust = tk.BooleanVar()
        def check_dust_command():
            global isDust
            isDust = boolCheckBtnDust.get()
        self.checkDust = tk.Checkbutton(variable=boolCheckBtnDust, onvalue=True, offvalue=False,
                                        command=check_dust_command, text="Dust")
        self.checkDust.pack(side=LEFT, padx=2)

        # check Vssl
        boolCheckBtnVssl = tk.BooleanVar()
        def check_vssl_command():
            global isVssl
            isVssl = boolCheckBtnVssl.get()

        self.checkVssl = tk.Checkbutton(variable=boolCheckBtnVssl, onvalue=True, offvalue=False,
                                        command=check_vssl_command, text="Vssl")
        self.checkVssl.pack(side=LEFT, padx=2)

        self.text = tk.Text(self)
        self.text.insert(tk.END, "")
        self.text.pack(side="left", fill="both")

        self.scroll_y = tk.Scrollbar(self, orient="vertical", command=self.text.yview)
        self.scroll_y.pack(side="left", fill="both")

        self.text.configure(yscrollcommand=self.scroll_y.set)

        # 메뉴 생성
        menubar = tk.Menu(root)
        filemenu = tk.Menu(menubar)
        filemenu.add_command(label="Save", command=self.funSave)
        filemenu.add_command(label="Save as...", command=self.funSaveas)
        filemenu.add_separator()
        filemenu.add_command(label="Exit", command=self.funQUIT)
        menubar.add_cascade(label="File", menu=filemenu)
        root.config(menu=menubar)

    # csv수집하는 보조스레드 호출
    def t_test(self):
        t1 = ThreadTest(self, self.q)
        t1.daemon = True
        t1.start()

    def tcp_thread(self):
        print("tcp_thread")
        self.t2.daemon = True
        self.t2.start()

    # 메인스레드, 보조스레드 종료
    def funQUIT(self):
        global quit_value
        quit_value = 0
        self.master.destroy()

    # menu_bar-file-save 기능함수
    def funSave(self):
        global filename
        if (filename == '.txt'):
            self.funSaveas()
        elif (filename[len(filename) - 3:] == 'txt'):
            with open(filename, 'w') as f:
                f.write(self.text.get("1.0", "end-1c"))
                f.close()
        else:
            self.funSaveas()

    # menu_bar-file-saveas 기능함수
    def funSaveas(self):
        global filename

        # 저장위치를 사용자가 임의로 정할 수 있도록 돕는 코드
        filename = filedialog.asksaveasfilename(initialdir="/", title="select file",
                                                filetypes=(("text files", "*.txt"),
                                                           ("all files", "*.*")))
        if filename == '':
            return 0

        # 저장시 자동으로 .txt
        filename = filename + '.txt'

        # 저장
        with open(filename, 'w') as f:
            f.write(self.text.get("1.0", "end-1c"))
            f.close()


# [x]버튼으로 종료시 정상적으로 보조스레드까지 종료시킬 수 있는 기능의 코드
def doSomething():
    global quit_value
    quit_value = 0
    root.destroy()


if __name__ == '__main__':
    root = tk.Tk()
    root.protocol('WM_DELETE_WINDOW', doSomething)
    app = Application(master=root)
    app.mainloop()
