import socket
import os

# IP = socket.gethostbyname(socket.gethostname())
IP = "192.168.2.10"
PORT = 4456
ADDR = (IP, PORT)
FORMAT = "utf-8"
SIZE = 4096
client_data = "./client_data/scg/data/malware/"


def main():
    for filename in os.listdir(client_data):
        # while True:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        data = client.recv(SIZE).decode(FORMAT)
        print(data)
        f = os.path.join(client_data, filename)
        if os.path.isfile(f):
            print("Uploading: ", f)
            with open(f"{f}", "r") as file:
                text = file.read()
        send_filename = f"{filename}"
        client.send(send_filename.encode(FORMAT))
        print(f"{client.recv(SIZE).decode(FORMAT)}")
        send_data = f"{text}"
        #print(send_data)
        client.sendall(send_data.encode(FORMAT))
        # ack1 = client.recv(SIZE).decode(FORMAT)
        # print(f"Server ACK1: {ack1}")
        print(f"UPLOADED: {filename}")
        client.close()
        # break
        # if ack1:


'''
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)

    while True:
        data = client.recv(SIZE).decode(FORMAT)
        cmd, msg = data.split("@")

        if cmd == "DISCONNECTED":
            print(f"[SERVER]: {msg}")
            break
        elif cmd == "OK":
            print(f"{msg}")

        #data = input("> ")
        #data = data.split(" ")
        #cmd = data[0]

        for filename in os.listdir(client_data):
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(ADDR)
            f = os.path.join(client_data, filename)
            if os.path.isfile(f):
                print("Uploading: ", f)
                with open(f"{f}", "r") as file:
                    text = file.read()

                #filename = client_data.split("/")[-1]
                #print(filename)
            #print(text)
            send_filename = filename
            print(filename)
            client.send(send_filename.encode(FORMAT))
            send_data = text
            client.send(send_data.encode(FORMAT))

    print("Disconnected from the server.")
    client.close()

'''
'''            
        if cmd == "UPLOAD":
            path = client_data
            #print(path)

            with open(f"{path}", "r") as f:
                text = f.read()

            filename = path.split("/")[-1]
            #print(filename)
            send_data = f"{cmd}@{filename}@{text}"
            client.send(send_data.encode(FORMAT))
'''

if __name__ == "__main__":
    main()
