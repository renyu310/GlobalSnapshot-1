import json
import time
import socket
import random
import threading
import uuid

class Snapshot:
    '''
    A Snapshot object. Contains state information
    for the state of the Peer (It's balance).

    Also contains information of the receive buffers of the
    peer.
    '''

    def __init__(self, marker, peer_state = 0):

        self.id = marker.id
        self.peer_state = peer_state
        self.recv_buffers = {}
        self.initiator = marker.initiator

    def __eq__(self, other):
        return self.id == other.id

    def __str__(self):
        string =  "Snapshot {}".format(str(self.id))
        string += "\n Balance : {}".format(self.peer_state)
        for k,v in self.recv_buffers.items():
            string += "{} <--- {} : {} ".format(socket.getfqdn(), str(k), str(v))
        return string



    def reg_recieve(self, peer_name, request):
        '''
        Registers a recieve event on one of the peer buffers.

        :param peer_name:  Name of the peer who has sent the message
        :type peer_name:   String
        :return:           None
        :rtype:            None
        '''

        if peer_name not in self.recv_buffers.keys():
            self.recv_buffers[peer_name] = []
            self.recv_buffers[peer_name].append(request)
        else:
            self.recv_buffers[peer_name].append(request)
    def __hash__(self):
        return hash(self.id)



class Marker:
    '''
    A marker message object. Has a unique id, that also
    applies to the snapshot it initiates.
    '''

    def __init__(self,message_dict = None):
        '''
        We can initialize a marker through a
        dictionary or manually.

        :param message_dict:
        :type message_dict:
        :return:
        :rtype:
        '''
        if message_dict:
            self.id = message_dict['id']
            self.initiator = message_dict['initiator']
            pass
        else:
            self.id = str(uuid.uuid4())
            self.initiator = socket.getfqdn()

    def __eq__(self, other):
        '''
        Two markers are equal if they have the same
        uuid.

        :param other:
        :type other:
        :return:
        :rtype:
        '''
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)



class Peer:
    '''
    A peer in our network. Initally setup with a balance of $1000.
    Has a thread that listen's for incoming connections on (0.0.0.0,8763).

    Has a thread that sends a deposit to another peer of a random amount of money
    every 1 second.

    Can initiate a snapshot, following which it stops all sending operations till
    we recieve a marker from all peers.
    '''

    def __init__(self):
        '''

        :return:
        :rtype:
        '''

        self.address = ('0.0.0.0',8763)
        self.PORT = 8763
        self.BALANCE = 1000
        self.PEER_LIST = {'hendrix.cs.rit.edu':[],
                          'doors.cs.rit.edu':[],
                          'glados.cs.rit.edu':[]
                          }


        #A dictionary mapping a marker to the list of peers who will
        #reply to the same.
        self.markers_seen = {}

        #To store completed snapshots
        self.snapshot_history = {}

        #To store currrently active snapshots
        self.active_snapshots = {}
        self.current_snapshot = None
        self.previous_snapshot = None


        self.listener = self.create_listener(self.address)
        thread_listener = threading.Thread(target=self.accept_connections, args=[self.listener], daemon= True)
        thread_listener.start()

        for i in range(10,-1,-1):
            print("{}..".format(i))
            time.sleep(1)

        thread_send_money = threading.Thread(target=self.send_money, daemon= True)
        thread_send_money.start()




    def accept_connections(self,listener):
        '''
        Accepts a connection and creates another thread to handle it.

        :param listener:    Our listening serversocket
        :type listener:     Socket
        :return:            None
        :rtype:             None
        '''

        while(True):
            client_socket,client_address = listener.accept()
            thread = threading.Thread(target=self.handle_connections, args=[client_socket], daemon= True)
            thread.start()



    def handle_connections(self,client_socket):
        '''
        Handles incoming client connections.

        :param client_socket:
        :type client_socket:
        :return:
        :rtype:
        '''

        request_dict = json.loads(client_socket.recv(1024).decode())
        request = request_dict['request']
        message_dict = request_dict['message']


        #If we have a process wanting to deposit money
        if request == 'DEPM':

            if self.active_snapshots:

                sender =    message_dict['sender']
                #print("Money recieved from: {}".format(sender))
                #Update channel state for all active snapshots
                for snapshot in self.active_snapshots.keys():
                    print("Update snapshot state for {}".format(snapshot))
                    self.active_snapshots[snapshot].reg_recieve(sender, message_dict['amount'])

                #Deposit money to balance
                self.deposit_money(message_dict)

            else:
                sender =    message_dict['sender']
                #print("Money recieved from: {}".format(sender))
                self.deposit_money(message_dict)


        #If we get a snapshot request
        elif request == 'MRKR':

            new_marker = Marker(message_dict)

            #If we havent seen the marker before
            if new_marker in self.markers_seen:

                self.handle_update_snapshot(message_dict)

            else:
                #self.markers_seen.append(new_marker)
                seen_from = message_dict['sender']
                print("Seen new marker request from {}".format(seen_from))
                #self.need_mark_reply.remove(seen_from)
                self.initiate_snapsot(new_marker, seen_from)




    def initiate_snapsot(self, marker, seen_from = None):
        '''


        :return:
        :rtype:
        '''

        print("Initiating Snapshot on {}".format(socket.getfqdn()))

        current_snapsot  = Snapshot(marker,self.BALANCE)

        #If the current snapshot isn't in the active snapshot dict
        #add it
        if current_snapsot not in self.active_snapshots.keys():
            self.active_snapshots[current_snapsot] = current_snapsot

        #Initialize need replies dict
        self.markers_seen[marker] = []

        peers = list(self.PEER_LIST.keys())

        #Remove oneself from peerlist
        peers.remove(socket.getfqdn())

        if seen_from:

            #Update whom we need to see a reply from
            need_mark_reply = peers.copy()
            need_mark_reply.remove(seen_from)
            #Create list for this particular marker
            self.markers_seen[marker] = need_mark_reply

        else:
            need_mark_reply = peers.copy()
            self.markers_seen[marker] = need_mark_reply


        #Forward Marker to all peers
        for peer in peers:

            send_dict = {'request':'MRKR', 'message':{
                'sender':socket.getfqdn(),
                'id':marker.id,
                'initiator':marker.initiator
            }}
            send_blob = json.dumps(send_dict).encode()
            send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("Sending marker to {}".format(peer))
            send_sock.connect((peer,self.PORT))
            send_sock.send(send_blob)





    def handle_update_snapshot(self, message_dict):
        '''
        Updates the snapshot with buffer values

        :param message_dict:
        :type message_dict:
        :return:
        :rtype:
        '''

        sender = message_dict['sender']
        marker = Marker(message_dict)

        if sender in self.markers_seen[marker]:
            self.markers_seen[marker].remove(sender)
        print("Seen marker from {}".format(sender))
        print("Now need to see reply from {}".format(self.markers_seen[marker]))

        if not self.markers_seen[marker]:
            snapshot= Snapshot(marker)
            current_snapshot = self.active_snapshots.pop(snapshot)
            print("Now done snaphot:")
            print(current_snapshot)
            self.snapshot_history[current_snapshot] = current_snapshot
            self.markers_seen.pop(marker)








    def create_listener(self,address):
        '''
        Creates a server socket on the specified address.

        :param address: (address,port)
        :type address: Tuple
        :return: server socket
        :rtype: Socket
        '''
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        listener.bind(address)
        listener.listen(12)
        return listener



    def deposit_money(self, message_dict):
        '''
        Deposits money by adding it to the balance.

        :return:
        :rtype:
        '''

        amount = message_dict['amount']
        self.BALANCE += amount
        #print("Updated balance : {}".format(self.BALANCE))






    def send_money(self):
        '''
        Will send dollars to another peer.
        Sends a random
        :return:
        :rtype:
        '''

        if self.BALANCE >0 :
            upper_limit = 100 if self.BALANCE>100 else self.BALANCE
            money_to_send = random.randint(0,100)
            peer_choices = list(self.PEER_LIST.keys())
            peer_choices.remove(socket.getfqdn())

            #Which peer we want to send money to. Chosen at random.
            choice = random.randint(0,len(peer_choices)-1)
            peer_choice = peer_choices[choice]

            message_dict = {'sender':socket.getfqdn(), 'amount':money_to_send}
            send_dict_blob = json.dumps({'request':'DEPM', 'message':message_dict}).encode()


            send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_sock.connect((peer_choice,self.PORT))
            send_sock.send(send_dict_blob)
            self.BALANCE -= money_to_send

            send_sock.close()
            #print("Sent {} to {}. Now Balance : {}".format(money_to_send, peer_choice, self.BALANCE))
            time.sleep(1)
            #Do it again..
            self.send_money()
        else:
            print("{} does not have any money".format(socket.getfqdn()))

if __name__ == '__main__':
    peer = Peer()
    while True:
        n = input()
        if n == '1':
            marker = Marker()
            thread = threading.Thread(target=peer.initiate_snapsot, args = [marker], daemon= True)
            thread.start()












