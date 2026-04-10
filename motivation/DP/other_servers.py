import socket
import pickle
import struct
import torch
import cv2

def receive_and_process_frames(port, root_host_ip, root_host_port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr}")

        data = b""
        payload_size = struct.calcsize("Q")

        while True:
            while len(data) < payload_size:
                packet = client_socket.recv(4*1024)
                if not packet:
                    break
                data += packet

            if not data:
                break

            packed_msg_size = data[:payload_size]
            data = data[payload_size:]
            msg_size = struct.unpack("Q", packed_msg_size)[0]

            while len(data) < msg_size:
                data += client_socket.recv(4*1024)

            frame_data = data[:msg_size]
            data = data[msg_size:]

            frame = pickle.loads(frame_data)
            processed_frame = process_frame_with_model(frame)

            send_processed_frame(processed_frame, root_host_ip, root_host_port)

        client_socket.close()

def process_frame_with_model(frame):
    tensor_frame = torch.from_numpy(frame).float().cuda()
    # 模型处理逻辑
    # model = ...
    # output = model(tensor_frame)
    # 这里只是示例，假设处理后的帧返回原始帧
    return frame

def send_processed_frame(frame, root_host_ip, root_host_port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((root_host_ip, root_host_port))
    
    data = pickle.dumps(frame)
    message = struct.pack("Q", len(data)) + data
    client_socket.sendall(message)
    client_socket.close()

if __name__ == "__main__":
    port = 9999
    root_host_ip = '192.168.1.1'
    root_host_port = 9998
    receive_and_process_frames(port, root_host_ip, root_host_port)