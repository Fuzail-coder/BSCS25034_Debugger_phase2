
// ======================= TIME-TRAVEL DEBUGGER - LOAD BALANCER TEMPLATE =======================

// What load balancer does:
//   1. Many clients can connect at the same time. Each one sends one string (the source code). 
//   2. Clients wait in a queue (first come, first served).
//   3. For each client, in order: make sure the server is running (start it with system() if not),
//      pass the string to the server, pass the server's reply back to the client.
//
// It does not read or understand the messages. It only moves bytes.
// The only messages it writes itself are errors ("server busy", "server failed to start").
//
// Two threads:
//   acceptor thread (acceptLoop) -> takes new clients and puts them in the queue. It never waits for the server.
//   main thread (dispatcherLoop) -> takes ONE client out of the queue and serves it completely, then the next.
//
// How the server is started:
//   system("./server 5001 &");
//   The "&" is required. Without it, system() waits until the server exits and the load balancer freezes.
//
//
// What the server program must do (the load balancer depends on this):
//   - it takes its port as the first argument, for example: ./server 5001
//   - it reads the string until the connection's write side is closed (recv returns 0)
//   - it sends its reply as [status: 1 byte][payload], then closes the connection 


#include <iostream>
#include <string>
#include <cstdlib>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "helper.h"
#include <cerrno>
#include <ctime>
#include <cstring>
#include <sys/time.h>
using namespace std;


// ---- Constants ----
const char *SERVER_PATH = "./server";      // the program we start when the server is down
const int IO_BUFFER_SIZE = 64 * 1024;      // fixed buffer for moving bytes
const int LISTEN_BACKLOG = 16;             // connections the OS holds until accept() takes them
const int MAX_PENDING_CLIENTS = 8;         // clients allowed to wait in our queue. More than this get "server busy".
const int STARTUP_MAX_ATTEMPTS = 50;       // after starting the server, how many times we try to connect
const int STARTUP_POLL_INTERVAL_MS = 100;  // wait between those tries (50 x 100 ms = 5 seconds in total)
int lbPort = 5000;
const int CLIENT_TIMEOUT_SECONDS = 30;
string serverHost = "127.0.0.1";
int serverPort = 5001;


// ---- Queue: here it holds client sockets (int). ----
// Implement it yourself, do not use std::queue.
template <typename T>
class Queue
{
    struct Node
    {
        T data;
        Node *next;
        Node(const T &d) : data(d), next(nullptr) {}
    };
    Node *front, *rear;
    int count;


public:
    // Implement these functions:
    Queue()
    {
        front = rear = nullptr;
        count = 0;
    }
    ~Queue()
    {
        while (front != nullptr)
        {
            Node *temp = front;
            front = front->next;
            delete temp;
        }
        rear = nullptr;
        count = 0;
    }
    void enqueue(const T &val)
    {
        Node* n = new Node(val);
        if(rear==nullptr)
        {
            front = rear = n;
            count++;
            return;
        }
        count++;
        rear->next = n;
        rear = n;

    }
    T dequeue()
    {
         if (isEmpty())
        {
            return T();
        }
        Node* n = front;
        T val = n->data; 
        front = front->next;
        if (front == nullptr)   
            rear = nullptr;
        delete n;
        count--;
        return val;
        
    }
    bool isEmpty()
    {
        return count ==0;
    }
    int size()
    {
        return count;
    }
};

struct ClientInfo
{
    int sock;           // the connected socket
    string address;     // the client's IP as text, e.g. "127.0.0.1"
    int port;           // the client's port
    time_t arrivedAt;   // when accept() returned
};

// The queue is used by two threads, so every use of it must be inside queueMutex.
Queue<ClientInfo> pendingClients;
mutex queueMutex;
sig_atomic_t flag = 1;


// ---- Basic socket helpers ----
bool sendAll(int sock, const char *buf, size_t n) 
{
    // Reply format: [status: 1 byte][sending_material][checksum: 4 bytes].
    // send() may send fewer bytes than asked for, so keep calling it until all n bytes are sent.
    size_t sent = 0;
    while(sent<n)
    {
        ssize_t s = send(sock,buf+sent,n-sent,MSG_NOSIGNAL);
        if(s<0)
        {
            if (errno == EINTR)
             continue;

            return false;
        }
        if (s == 0)
            return false;
        sent += (size_t)s;

    }
    return true;

}

bool sendError(int sock, const string &message)
{
    // Reply format: [status: 1 byte][payload until close][checksum: 4 bytes].
    // For an error: send one byte with value 1, then the message text. 
     char status = 1;                       // 1 = error
     uint32_t sum = 0;
    if (!sendAll(sock, &status, 1))
        return false;
    if (!message.empty() && !sendAll(sock, message.data(), message.size()))
        return false;
    unsigned char t[4];
    encodeU32(sum, t);
    return sendAll(sock, (const char *)t, 4);

}

int createListenSocket(int port)
{ 
    // Create a TCP socket with SOCK_CLOEXEC(flag).
    // set SO_REUSEADDR so we can restart the load balancer right away.
    // bind() to the port on any address, then listen(LISTEN_BACKLOG).
    // If bind or listen fails, print the error and exit, because without this socket we cannot work.
    int s = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0); // socket(domain, type, protocol);
    if (s < 0)
    {
        perror("socket");
        exit(1);
    }
    int yes = 1;
    //setsockopt(socket, level, option, value, size);
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
        perror("setsockopt(SO_REUSEADDR)");
    
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    //bind(socket, address, address_length);
    if (bind(s, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("bind");
        close(s);
        exit(1);
    }
    if (listen(s, LISTEN_BACKLOG) < 0)
    {
        perror("listen");
        close(s);
        exit(1);
    }
    return s;
}
int connectToServer(const char *host, int port)
{
    // Create a TCP socket (SOCK_CLOEXEC) and connect it to host:port.
    // Return the socket, or -1 if the connection fails 
    int s = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (s < 0)
        return -1;
    
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1)
    {
        close(s);
        return -1;
    }
    if (connect(s, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        close(s);
        return -1;
    }
    return s;
}

// =============================================================================================
// PART 1: WAKING UP THE SERVER IF IT IS DOWN
// =============================================================================================


bool startServer(int port)
{
    // Build the command text: SERVER_PATH + " " + port as text + "&"
    //   Example: "./server 5001 &"
    // Call system() with it.
    // Return true if system() returned 0, otherwise false.
    string cmd = string(SERVER_PATH) + " " + to_string(port) + " &";
    int rc = system(cmd.c_str());
    return rc == 0;
}
int connectToServerWithWakeup()
{
    // 1. Try connectToServer(). If it works, return that socket. The server is alive.
    int s = connectToServer(serverHost.c_str(), serverPort);
    if (s >= 0)
        return s;
    // 2. Otherwise call startServer(). If it fails, return -1.
     if (!startServer(serverPort))
        return -1;
    // 3. Try connectToServer() up to STARTUP_MAX_ATTEMPTS times 
    for (int i = 0; i < STARTUP_MAX_ATTEMPTS; i++)
    {
        usleep(STARTUP_POLL_INTERVAL_MS * 1000);  
        s = connectToServer(serverHost.c_str(), serverPort);
        if (s >= 0)
            return s;                              
    }
    // 4. Return the first working socket, or -1 if the server never came up.
    return -1; 
}


// =============================================================================================
// PART 2: PASSING THE STRING AND THE REPLY (one client, called by the main thread only)
// =============================================================================================


void relayOneClient(ClientInfo &c)
{
    // 1. serverSock = connectToServerWithWakeup().
    //    If it is -1: sendError "backend server failed to start", close clientSock, return.
     int serverSock = connectToServerWithWakeup();
    if (serverSock < 0)
    {
        sendError(c.sock, "backend server failed to start");
        close(c.sock);
        return;
    }
     char buf[IO_BUFFER_SIZE];
    // 3. Client -> server: repeat recv() from clientSock into a buffer of IO_BUFFER_SIZE and sendAll() it to serverSock.
    //    Stop when recv() returns 0 (the client finished sending) or fails.
    bool uploadOk = true;
    while (true)
    {
        ssize_t r = recv(c.sock, buf, sizeof(buf), 0);
        if (r < 0)
        {
            if (errno == EINTR)
                continue;              
            uploadOk = false;
            break;
        }
        if (r == 0)
            break;                      
        if (!sendAll(serverSock, buf, (size_t)r))
        {
            uploadOk = false;
            break;
        }
    }
    if (!uploadOk)
    {
        sendError(c.sock, "failed to pass your request to the server");
        close(serverSock);
        close(c.sock);
        return;
    }

    // 4. Call shutdown(serverSock, SHUT_WR), so the server knows the string is complete.
    shutdown(serverSock, SHUT_WR);
    // 5. Server -> client: repeat recv() from serverSock and sendAll() to clientSock.
    //    Stop when recv() returns 0 (the server sent its whole reply and closed) or fails.

     while (true)
    {
        ssize_t r = recv(serverSock, buf, sizeof(buf), 0);
        if (r < 0)
        {
            if (errno == EINTR)
                continue;
            break;
        }
        if (r == 0)
            break;                     
        if (!sendAll(c.sock, buf, (size_t)r))
            break;                      
    }
    // 6. Close both sockets.
     close(serverSock);
     close(c.sock);
}


// =============================================================================================
// PART 3: THE TWO THREADS
// =============================================================================================


void acceptLoop(int listenSock)
{
    // Repeat: accept() a new client. 
    while (flag)
    {
        sockaddr_in caddr;
        socklen_t clen = sizeof(caddr);
        int clientSock = accept(listenSock, (sockaddr *)&caddr, &clen);
        if (clientSock < 0)
        {
            if (flag)
                perror("accept");     
            continue;
        }
         ClientInfo c;
        c.sock = clientSock;
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &caddr.sin_addr, ip, sizeof(ip));
        c.address = ip;
        c.port = ntohs(caddr.sin_port);
        c.arrivedAt = time(nullptr);
    //  If pendingClients.size() is already MAX_PENDING_CLIENTS: unlock, sendError "server busy, try again later",
    // else
    // Lock queueMutex.
        bool full;
        {
            lock_guard<mutex> lk(queueMutex);
            if (pendingClients.size() >= MAX_PENDING_CLIENTS)
                full = true;
            else
            {
                pendingClients.enqueue(c);
                full = false;
            }
        }
    //   close the client, and go on.
    //   Otherwise enqueue the client socket.
    // Never wait for the server here, so new clients are always accepted quickly.
    // If accept() fails, print the error and keep going (do not end the thread).
        if (full)
        {
            sendError(clientSock, "server busy, try again later");
            shutdown(clientSock, SHUT_WR);           // hamara reply poora
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;                 
            setsockopt(clientSock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            char junk[IO_BUFFER_SIZE];
            while (recv(clientSock, junk, sizeof(junk), 0) > 0)
            {
            }                                       
            close(clientSock);
        }
    }
}
void dispatcherLoop()
{
    // This runs in the main thread and never ends.
    // Repeat:
    //   Lock queueMutex, take ONE client out of the queue if there is one, unlock.
    //   If we got a client: call relayOneClient(). The next client is taken out of the queue only after this one is finished.
    while (flag)
    {
        // Lock ke andar sirf ek client nikalo
        ClientInfo c;
        bool got = false;
        {
            lock_guard<mutex> lk(queueMutex);
            if (!pendingClients.isEmpty())
            {
                c = pendingClients.dequeue();
                got = true;
            }
        }

        if (!got)
        {
            usleep(10 * 1000);       
            continue;
        }

        if (time(nullptr) - c.arrivedAt > CLIENT_TIMEOUT_SECONDS)
        {
            close(c.sock);
            continue;
        }

        relayOneClient(c);           
    }
}

void handler(int sg) {
    (void)sg;
    flag = 0;
}
int main(int argc, char *argv[])
{

    if (argc >= 3)
    {
        serverHost = argv[1];
        serverPort = atoi(argv[2]);
    }
    signal(SIGINT, handler); // whenever ctrl+c will be pressed to exit the program the flag wll be set so insead of using infinite loop you can loop on flag to safely close things and exit
    // 2. listenSock = createListenSocket(LB_PORT).
     int listenSock = createListenSocket(lbPort);
    // 3. Start a thread that runs acceptLoop(listenSock) and detach it.
    thread acceptor(acceptLoop, listenSock);
    acceptor.detach();
    // 4. Call dispatcherLoop().
    dispatcherLoop();
    {
        lock_guard<mutex> lk(queueMutex);
        while (!pendingClients.isEmpty())
        {
            ClientInfo c = pendingClients.dequeue();
            close(c.sock);
        }
    }
    close(listenSock);

    return 0;
}