#서버 코드
import datetime
import threading
import socket
import time
import tkinter as tk

ThisId = "Server"
Spliter = '!'


class Room: #채팅방
    def __init__(self):
        # 접속한 클라이언트를 담당하는 ChatClient 객체 저장
        self.clients = []
        self.clients_dict = dict()

    # 클라이언트 하나를 채팅방에 추가
    def add_client(self, c, id_str: str):
        self.clients.append(c)
        self.clients_dict[id_str] = c

    # 클라이언트 하나를 채팅방에서 삭제
    def del_client(self, c, id_str: str):
        self.clients.remove(c)

    def send_all_clients(self, msg):
        for key in self.clients_dict.keys():
            self.clients_dict[key].send_msg(msg)

    def send_clients(self, msg, client):
        try:
            self.clients_dict[client].send_msg(msg)

        except Exception as e:
            print(e)


class ChatClient:#텔레 마케터: 클라이언트 1명이 전송한 메시지를 받고, 받은 메시지를 다시 되돌려줌
    def __init__(self, soc, r):
        self.id = id    #클라이언트 id
        self.soc = soc  #담당 클라이언트와 1:1 통신할 소켓
        self.room = r   #채팅방 객체

    def recv_msg(self):

        while True:
            try:
                data = self.soc.recv(1024)
                message = data.decode()
                message = message.strip()
                print(f"recvMsg receive : {message} \ntime:{datetime.datetime.now().strftime('%m/%d/%Y, %H:%M:%S')}")
            except ConnectionResetError as e:
                self.app.text.insert(tk.END, f"ip: {self.id} del in Node\n")
                self.room.del_client(self)
                break

    # 담당한 클라이언트 1명에게만 메시지 전송
    def send_msg(self, msg):
        self.soc.sendall(msg.encode(encoding='utf-8'))

    def run(self):
        t = threading.Thread(target=self.recv_msg, args=())
        t.start()


class ServerMain(threading.Thread):
    # '192.168.0.11'
    ip = 'localhost'
    print(f"ip: {ip}")
    port = 4444

    def __init__(self, app, q):
        threading.Thread.__init__(self)
        self.q = q
        self.app = app
        self.room = Room()
        self.server_soc = None

    def open(self):
        self.server_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_soc.bind((ServerMain.ip, ServerMain.port))
        self.server_soc.listen()

    def run(self):
        self.open()
        print('채팅 서버 시작')
        while True:
            c_soc, addr = self.server_soc.accept()
            print(addr)

            cc = ChatClient(c_soc, self.room)
            cc.send_msg("S1!/checkId!0\n")
            print(f"send : {'S1!/checkId!0'}\n")

            # check ID start
            data = c_soc.recv(1024)
            message = data.decode()
            message = message.strip()
            print(f"receive : {message}")
            if message.find("!"):
                splitList = message.split('!')
                nodeId = splitList[0]
                order = splitList[1]
            else:
                nodeId = "/dummy"
                order = "/dummy"

            # client
            if order == "/sendId" and nodeId[0] == "T":
                self.room.add_client(cc, nodeId)
                print(f'ip: {addr} add in Node \n채팅 시작')
                self.app.text.insert(tk.END, f"ip: {nodeId} add in Node\n")
                cc.run()
            # check Id End

            print(f"clients_dict :\n{self.room.clients_dict}")


# if __name__ == '__main__':
#     server = ServerMain()
#     # server.room.send_all_clients("adf")
#     server.run()
