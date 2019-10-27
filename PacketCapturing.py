import socket
import pye
import struct

Socket=socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
while True:
  packet = Socket.recvfrom(65565)
  ip_header = struct.unpack("!BBHHHBBH4s4s", packet[0][0:20])
  source_ip = socket.inet_ntoa(ip_header[8])
  destination_ip = socket.inet_ntoa(ip_header[9])
  total_length = ip_header[2]
  print("source: ", source_ip, ", dest: ", destination_ip, \
        ", total length: ", total_length)
