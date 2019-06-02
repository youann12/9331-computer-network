## python vision 3.6.3
from socket import *
import time
import sys
import os
import threading
import struct
import random


identify = str(sys.argv[1])
successive1 = str(sys.argv[2])
successive2 = str(sys.argv[3])
mss = int(str(sys.argv[4]))
drop_rate = float(str(sys.argv[5]))
predecessor = -1
port = int(str(50000 + int(identify)))
start_time = time.time()
first = -1
predlist = []
ack = 1
ackconfirm = 1
switch = True

seq_num1 = 1
seq_num2 = 1

res_num1 = 1
res_num2 = 1

peer_has = -1
#for ping socket
# for the ping request
def ping_request():
    global port
    global successive1
    global successive2
    global switch
    global seq_num1
    global seq_num2
    global res_num1
    global res_num2
    ip = '127.0.0.1'

    s_ping = socket(AF_INET,SOCK_DGRAM)
    while switch:
        port1 = int(str(50000 + int(successive1)))
        port2 = int(str(50000 + int(successive2)))
        address1 = (ip,port1)
        address2 = (ip,port2)
        massage = ("ping" + str(port - 50000) +"\n"+ successive1 + "\n" + str(seq_num1)).encode()
        s_ping.sendto(massage,address1)
        s_ping.sendto(massage,address2)
        time.sleep(5)
        seq_num1 += 1
        seq_num2 += 1
        if seq_num1 - res_num1 > 3:
            print(f"Peer {successive1} is no longer alive.")
            s_tcp = socket(AF_INET,SOCK_STREAM)
            mes = ("kill" + successive1 + "\n" + str(port - 50000)).encode()
            s_tcp.connect(address2)
            s_tcp.send(mes)
            s_tcp.close()
            seq_num1 = 1
            res_num1 = 1
            seq_num2 = 1
            res_num2 = 1
        time.sleep(1)

        if seq_num2 - res_num2 > 3:
            print(f"Peer {successive2} is no longer alive.")
            s_tcp = socket(AF_INET,SOCK_STREAM)
            mes = ("kill" + successive2 + "\n" + str(port - 50000)).encode()
            s_tcp.connect(address1)
            s_tcp.send(mes)
            s_tcp.close()
            seq_num2 = 1
            res_num2 = 1
            time.sleep(1)
    s_ping.close() 

# for the udp response
def ping_response(port):
    global switch
    global predecessor
    global ack
    global peer_has
    global first
    global identify
    global predlist
    global seq_num
    global successive1
    global successive2
    global res_num1 
    global res_num2 
    
    s_ping = socket(AF_INET,SOCK_DGRAM)
    ip = '127.0.0.1'
    address = (ip,port)
    s_ping.bind(address)
    while switch:
        res_dic = {}
        request,add = s_ping.recvfrom(1024)
        request = request.decode()
        if request[:4] == "ping":
            rs = request.split('\n')
            r = rs[0][4:]
            s1 = rs[1]
            seqq = rs[2]

            print(f"A ping request message was received from Peer {str(r)}.")
            if int(r) not in predlist:
                predlist.append(int(r))
            m = ("resp" + str(port - 50000) + "\n" + seqq).encode()
            s_res = socket(AF_INET,SOCK_DGRAM)
            addressres = (ip,int(r) + 50000)
            s_res.sendto(m,addressres)
            if s1 == str(port - 50000):
                predecessor = int(r)
                if int(predecessor) > int(identify):
                    first = 1
                else:
                    first = 0
        elif request[:4] == "resp":
            rss = request.split('\n')
            rr = rss[0][4:]
            res_num = int(rss[1])
            if rr == successive1:
                res_num1 = res_num
            elif rr == successive2:
                res_num2 = res_num
            print(f"A ping response message was received from Peer {str(rr)}.")
    s_ping.close()
    
def hash_file(file_name):
    the_name = file_name % 256
    return the_name
    
def file_request():
    global port
    global successive1
    global successive2
    global predlist
    global switch
    ip = '127.0.0.1'
    port1 = int(str(50000 + int(successive1)))
    address1 = (ip,port1)
    while switch:
        file = input()
        if len(file) == 0:
            continue
        elif len(file) > 0 and file[:7] == "request":
            s_ping = socket(AF_INET,SOCK_STREAM)
            s_ping.connect(address1)       
            file_name = int(file[8:])
            massage = ("file" + str(file_name) + str(port - 50000) +"\r\n").encode()
            s_ping.send(massage)
            print(f"File request message for {str(file_name)} has been sent to my successor.")
            s_ping.close()
        elif len(file) > 0 and file[:4] == "quit":
            s_ping = socket(AF_INET,SOCK_STREAM)
            addresspre0 = (ip,predlist[0] + 50000)
            addresspre1 = (ip,predlist[1] + 50000)
            massage = ("quit" + str(int(port - 50000)) + "\n" + str(successive1) + "\n" + str(successive2)).encode()
            massage2 = ("igo" + str(int(port - 50000)) + "\n").encode()
            ad = (ip,int(successive1) + 50000)
            s_ping.connect(ad)
            s_ping.send(massage2)
            s_ping.close()
            s_ping = socket(AF_INET,SOCK_STREAM)
            s_ping.connect(addresspre0)
            s_ping.send(massage)
            s_ping.close()
            s_ping = socket(AF_INET,SOCK_STREAM)
            s_ping.connect(addresspre1)
            s_ping.send(massage)
            s_ping.close()
            switch = False
            



            
## for TCP response
def file_response():
    global port
    global successive1
    global successive2
    global predecessor
    global peer_has
    global first
    global switch
    global predlist
    ip = '127.0.0.1'
    address = (ip,port)
    s_ping = socket(AF_INET,SOCK_STREAM)
    s_ping.bind(address)
    s_ping.listen(5)
    while switch:
        conn = s_ping.accept()[0]
        request = conn.recv(1024)
        if request.decode("utf-8")[:5] == "Ihave":# find some peer has the file
            file_receive = request.decode("utf-8")[5:9]
            peer_has = request.decode("utf-8")[9:-2]
            print(f"Received a response message from peer {peer_has}, which has the file {file_receive}.")
            print(f"We now start receiving the file .........")
            peer_has = int(peer_has)

            #request 2012
        elif request.decode("utf-8")[:4] == "file":
            file = request.decode("utf-8")[4:8]
            filehash = hash_file(int(request.decode("utf-8")[4:8]))
            the_requester = request.decode("utf-8")[8:-2]
            if (filehash > predecessor and filehash <= int(identify)) or (first == 1 and filehash > predecessor):
                print(f"File {str(file)} is here.")
                addressr = (ip,int(the_requester) + 50000)
                s_file_response = socket(AF_INET,SOCK_STREAM)
                s_file_response.connect(addressr)
                massage = ("Ihave" + str(file) + str(port - 50000) +"\r\n").encode()
                s_file_response.send(massage)
                s_file_response.close()
                print(f"A response message, destined for peer {the_requester}, has been sent.")
                print("We now start sending the file .........")
                f_trans(int(the_requester) + 60000,file)
        
            else:
                print(f"hashfile = {filehash}")
                print(f"File {str(file)} is not stored here.")
                address2 = (ip,int(successive1) + 50000)
                s_ping2 = socket(AF_INET,SOCK_STREAM)
                s_ping2.connect(address2)
                s_ping2.send(request)
                s_ping2.close()
                print(f"File request message has been forwarded to my successor.")
                
        elif request.decode("utf-8")[:4] == "quit":
            # massage = ("quit" + str(int(port - 50000)) + "\n" + str(successive1) + "\n" + str(successive2)).encode()
            request = request.decode().split('\n')
            the_quiter = int(request[0][4:])
            the_q1 = int(request[1])
            the_q2 = int(request[2])
            print(f"Peer {str(the_quiter)} will depart from the network.")
            if str(successive1) == str(the_quiter):
                successive1 = str(the_q1)
                print(f"My first successor is now peer {str(the_q1)}.")
                successive2 = str(the_q2)
                print(f"My second successor is now peer {str(the_q2)}.")
            elif str(successive2) == str(the_quiter):
                successive2 = str(the_q1) 
                print(f"My first successor is now peer {str(successive1)}.")
                print(f"My second successor is now peer {str(successive2)}.")
        elif request.decode("utf-8")[:4] == "kill":
            # mes = ("kill" + successive2 + "\n" + str(port - 50000))

            request = request.decode().split('\n')
            killer = request[1]
            mes = ("noworry" + str(port - 50000) + "\n" + str(successive1) + "\n" + str(successive2)).encode()
            address2 = (ip,int(killer) + 50000)
            s_ping2 = socket(AF_INET,SOCK_STREAM)
            s_ping2.connect(address2)
            s_ping2.send(mes)
            s_ping2.close()
        elif request.decode("utf-8")[:7] == "noworry":

            # mes = ("noworry" + str(port - 50000) + "\n" + str(successive1)).encode()
            request = request.decode().split('\n')
            s = request[0][7:]
            s1 = request[1]
            s2 = request[2]
            if s == successive2:
                s_ping2 = socket(AF_INET,SOCK_STREAM)
                address = (ip, int(s) + 50000)
                s_ping2.connect(address)
                mes = ("igo" + successive1).encode()
                s_ping2.send(mes)
                s_ping2.close()
                successive1 = s
                successive2 = s1

                print(f"My first successor is now peer {successive1}.")
            elif s == successive1:
                successive2 = s2
                print(f"My first successor is now peer {successive1}.")
                print(f"My second successor is now peer {successive2}.")
        elif request.decode("utf-8")[:3] == "igo":
            #"igo" + str(int(port - 50000)) + "\n"
            request = request.decode()
            left = int(request[3:])
            if left in predlist:
                predlist.remove(left)

            
    
def f_trans(des,file):
    global mss
    global start_time
    global drop_rate
    global switch
    global ack
    global ackconfirm
    
    ip = '127.0.0.1'
    address1 = (ip,des)
    s_ping = socket(AF_INET,SOCK_DGRAM)
    sent = 0
    file = str(file) + ".pdf"
    fsize = os.path.getsize(file)
    seq = 1
    
    with open(file,"rb") as f:
        while sent < fsize:
            if ack == ackconfirm:
                header = bytes(("file" + str(port + 10000) + "\n" + str(seq) + "\n").encode())
                ff = f.read(mss)
                send_file = header + ff
                logfile = open("responding_log.txt","a")
                flag = 0
                
                while random.random() <= drop_rate:
                    t = str(time.time() - start_time)
                    if flag == 0:
                        event = "Drop"
                    elif flag > 0:
                        event = "RTX/Drop"
                    l = len(ff)
                    a = "0"
                    logfile.write("%-10s %20s %10s %10s %10s \n" % (event,t,str(seq),l,a))
                    logfile.flush()
                    flag += 1
                       
                sent = sent + mss - len(header)
                logfile = open("responding_log.txt","a")
                
                t = str(time.time() - start_time)
                if flag == 0:
                    event = "snd"
                elif flag > 0:
                    event = "RTX"
                l = len(ff)
                a = "0"
                logfile.write("%-10s %20s %10s %10s %10s \n" % (event,t,str(seq),l,a))
                logfile.flush()
                s_ping.sendto(send_file,address1)
                flag = 0
                seq += l
                ack = seq
            else:
                time.sleep(1)
            #time.sleep(1)
    message = ("over" + "\r\n").encode()
    s_ping.sendto(message,address1)
    print("The file is sent.")
    s_ping.close() 

# for the udp response
def f_rec():
    global port
    global start_time
    global ackconfirm
    global switch
    s_ping = socket(AF_INET,SOCK_DGRAM)
    s_ack = socket(AF_INET,SOCK_DGRAM)
    ip = '127.0.0.1'
    address = (ip,port + 10000)
    s_ping.bind(address)

    while switch:
        request,add = s_ping.recvfrom(1024)
        if request[:4] == b'file':
            a1 = request.split(b'\n')[0]
            des = int(a1[4:].decode())
            a2 = request.split(b'\n')[2:]
            s = request.split(b'\n')[1].decode()
            a3 = b'\n'.join(a2)
            recfile = open("receive.pdf","ab")
            recfile.write(a3)
            recfile.flush()
            logfile = open("requesting_log.txt","a")
            t = str(time.time() - start_time)
            l = len(a3)
            ack = l + int(s)
            event = "rcv"
            logfile.write("%-10s %20s %10s %10s %10s \n" % (event,t,str(s),str(l),"0"))
            logfile.flush()
            message = ("ack" + str(ack) + "\n" + str(l)).encode()
            addressfrom = (ip,des)
            s_ack.sendto(message,addressfrom)
            event = "snd"
            logfile.write("%-10s %20s %10s %10s %10s \n" % (event,t,"0",str(l),ack))
            logfile.flush()
        elif request[:4] == b'over':
            print("The file is received.")
            
        elif request[:3] == b'ack':
            request = request.split(b'\n')
            l = request[1].decode()
            ax = request[0].decode()[3:]
            logfile = open("responding_log.txt","a")
            event = "rcv"
            t = str(time.time() - start_time)
            ackconfirm = int(ax)
            logfile.write("%-10s %20s %10s %10s %10s \n" % (event,t,"0",str(l),ax))
            logfile.flush()   
    s_ping.close() 
# for listen and request in the same time
threads = []
req = threading.Thread(target = ping_request)
threads.append(req)
frec = threading.Thread(target = f_rec)
threads.append(frec)
tcp_req = threading.Thread(target = file_request)
threads.append(tcp_req)
tcp_res = threading.Thread(target = file_response)
threads.append(tcp_res)
resp = threading.Thread(target = ping_response, args = (port,))
threads.append(resp)

if __name__=='__main__':
    for t in threads:
        t.start()
    for t in threads:
        t.join()
