import socket

TCP_IP = 'localhost'
TCP_PORT = 6100



MESSAGE = "Test data Test data Test data"

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, TCP_PORT))
s.send(MESSAGE)
s.close()