import socketserver
import threading
from pathlib import Path
from queue import Queue, Empty
import time
import select

class dataclient(socketserver.BaseRequestHandler):
    
    def setup(self):
        self.ALL = b""

    def handle(self, *args):	
        
        infoStr = self.request.recv(256).decode()
        metavals = infoStr.split() 
        self.fsize =int( metavals[0] ) 
        #self.fpath = os.path.dirname( metavals[1] )
        #self.fname = os.path.basename( metavals[1] )
        self.val = int( metavals[2] )
        self.imwidth = int( metavals[3] ) 
        self.imheight = int( metavals[4] )
        self.val2 = int( metavals[4] )
        if metavals[1].startswith('!'):
            self.clobber = True
            self.path = Path(metavals[1:])
        else:
            self.clobber = False
            self.path = Path(metavals[1])

        while self.fsize != len(self.ALL):
            #grab the first 256 bytes
            #which is metadata. 
            
            data = self.request.recv(1024)

            self.infoStr = data
            


            
#            if self.fpath.startswith( '!' ) :
#                self.clobber = True
#                self.fpath = self.fpath[1:]


            self.ALL+=data

        if not self.clobber and self.path.exists():
            tmppath = Path("/tmp")/self.path.name
            print("No clobber and file exists!")
            self.path = tmppath

        print(f"Writting to {self.path}")
        with self.path.open("wb") as fd:
            fd.write(self.ALL)



    def finish(self):
        print("finishing")
        print(self.server.analysis_server.get_queues())
        for aclient_q in self.server.analysis_server.get_queues():
            print("putting")
            aclient_q.put(str(self.path))
            
class _analysis_server(socketserver.ThreadingTCPServer):

    def __init__(self, *args):
        super().__init__(*args)
        self.queuelist=[]

    def regster_queue(self, name, q):
           
        self.queuelist[name] = q

    def remove_queue(self, name):
        del self.queuelist[name]

    def get_queues(self):
        return self.queuelist


class _analysis_client(socketserver.BaseRequestHandler):

    def setup(self):
        self.image_ready = Queue()
        self.server.queuelist.append(self.image_ready)

    def handle(self, *args):
        counter = 1
        client = self.request
        while 1:
            r,w,x = select.select([client], [client], [client], 0 )
            # I thought select would return empty list in w if 
            # the client closed the socket. In my tests with 
            # netcat this doesn't happen. The only fix I can 
            # see is to attempt to send the user a 0 and catch
            # the error.

            try:
                client.send(b"\x00")
            except(BrokenPipeError, IOError):
                break

            if len(w) == 0:
                break;

            counter+=1
            try:
                resp = self.image_ready.get(block=False)
            except Empty:
                
                continue

            client.send(resp.encode())
            client.send(b"\n")
                
        client.close()

    def finish(self):
        print("removing queue")
        self.server.queuelist.remove(self.image_ready)

class _dataserver(socketserver.ThreadingTCPServer):
    """Why subclass you ask? B/C I am a follower:
    https://docs.python.org/3.7/library/socketserver.html
    """

    def aserver(self, analysis_server):
        self.analysis_server = analysis_server


def main():
    HOST, PORT = "", 6543
    analysis_server = _analysis_server(("", 6540), _analysis_client)
    dataserver = _dataserver((HOST, PORT), dataclient)
    dataserver.aserver(analysis_server)

    dataserver_thread = threading.Thread(target=dataserver.serve_forever)
    dataserver_thread.start()
    
    
    analysis_server_thread = threading.Thread(target=analysis_server.serve_forever)
    analysis_server_thread.start()
    
    try:
        while 1:
            time.sleep(2)
            print(dataserver.analysis_server.get_queues())
            pass       
    except KeyboardInterrupt:
        pass
        
    finally:
        print("shutting down")
        dataserver.shutdown()
        dataserver.server_close()

        print("shutting down")
        analysis_server.shutdown()
        analysis_server.server_close()


    
main()
