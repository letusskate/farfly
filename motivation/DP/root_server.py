import cv2
import socket
import pickle
import struct
import threading

def send_frames(video_path, host_ips, port):
    cap = cv2.VideoCapture(video_path)
    sockets = [socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in host_ips]
    for sock, ip in zip(sockets, host_ips):
        sock.connect((ip, port))

    frame_id = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        data = pickle.dumps(frame)
        message = struct.pack("Q", len(data)) + data
        target_sock = sockets[frame_id % len(host_ips)]
        target_sock.sendall(message)
        frame_id += 1

    for sock in sockets:
        sock.close()

def receive_processed_frames(port, output_video_path, frame_count):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)

    out = cv2.VideoWriter(output_video_path, cv2.VideoWriter_fourcc(*'mp4v'), 30, (width, height))

    frames_received = 0
    while frames_received < frame_count:
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
            out.write(frame)
            frames_received += 1
        
        client_socket.close()

    out.release()
    server_socket.close()

if __name__ == "__main__":
    video_path = 'path_to_your_video.mp4'
    output_video_path = 'output_video.mp4'
    host_ips = ['192.168.1.2', '192.168.1.3', '192.168.1.4', '192.168.1.5']
    port = 9999

    cap = cv2.VideoCapture(video_path)
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()

    send_thread = threading.Thread(target=send_frames, args=(video_path, host_ips, port))
    receive_thread = threading.Thread(target=receive_processed_frames, args=(port, output_video_path, frame_count))

    send_thread.start()
    receive_thread.start()

    send_thread.join()
    receive_thread.join()