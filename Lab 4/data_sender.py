import socket
import time
import numpy as np

def generate_data():
    x1 = np.random.rand()*10
    x2 = np.random.rand()*5
    noise = np.random.normal(0, 0.2)
    y = 2.5 * x1 + 1.8 * x2 + noise
    return f"{x1:.3f},{x2:.3f},{y:.3f}"

def start_data_server(host='localhost', port=9999):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        print(f"✅ Server phát dữ liệu đang lắng nghe tại {host}:{port}...")

        while True:
            conn, addr = s.accept()
            print(f"📡 Kết nối từ: {addr}")
            with conn:
                try:
                    while True:
                        data = generate_data()
                        conn.sendall((data + '\n').encode())
                        time.sleep(0.5)
                except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                    print("⚠️ Client đã ngắt kết nối. Dừng server...")
                    return

if __name__ == "__main__":
    start_data_server()
