import os
import socket
import threading

#IP = socket.gethostbyname(socket.gethostname())
IP = "192.168.2.10"
PORT = 4456
ADDR = (IP, PORT)
SIZE = 4096
FORMAT = "utf-8"
SERVER_DATA_PATH = "server_data"


def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    conn.send("OK@Welcome to the File Server.".encode(FORMAT))
    name = conn.recv(SIZE).decode(FORMAT)
    print(f"[RECEIVING] {name}")
    conn.send("[RECEIVED] FILE NAME".encode(FORMAT))

    filepath = os.path.join(SERVER_DATA_PATH, name)
    with open(filepath, "w") as f:
        while True:
            text = conn.recv(SIZE).decode(FORMAT)
            #print(text)
            f.write(text)
            if not text:
                break

    f.close()
    #receiving1 = f"RECEIVING CONTENT"
    #conn.send(receiving1.encode(FORMAT))

    print(f"[DISCONNECTED] {addr} disconnected")
    conn.close()


def main():
    print("[STARTING] Server is starting")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[LISTENING] Server is listening on {IP}:{PORT}.")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")


if __name__ == "__main__":
    main()
