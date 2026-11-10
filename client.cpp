// ======================= TIME-TRAVEL DEBUGGER - CLIENT TEMPLATE =======================

// The client never talks to the server directly. It only talks to the load balancer.
// The load balancer wakes the server up if it is down, so the client does not care.
// The first "start" may just take a bit longer.
//
// The network is used ONLY by "start". After the .tdbg file is downloaded,
// next / prv / jmp read that file on the local disk. They never contact the load balancer.
//
// How the pieces fit together:
//   main loop -> readCommand -> runCommand
//   start -> cmdStart()  -> requestDebugFile() (Part A) -> openTdbg() (Part B) -> shows step 1
//   next, prv, jmp -> moveBy() (Part D) -> readSnapshotAt() (Part B) -> printSnapshot() (Part E)

#include <iostream>
#include <string>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <sstream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <unistd.h>
#include <signal.h>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include "helper.h"
using namespace std;

// ---- Constants ----
const char *PROGRAM_NAME = "ttdb";
const int TDBG_HEADER_SIZE = 20;
const char *PROMPT = "ttdb> ";
const int IO_BUFFER_SIZE = 64 * 1024;     // fixed buffer for streaming files to and from the socket
const char *TDBG_FILE = "ttdb_last.tdbg"; // where the downloaded .tdbg is saved
const int MAX_JUMP = 1000000000;          // biggest n accepted by jmp
string LB_HOST = "127.0.0.0";
int LB_PORT = 5000;

// These two must be the same as in Phase 1

const int MAX_VARS_PER_FRAME = 16;
const int MAX_STACK_DEPTH = 64;


// ---- Structs (same as Phase 1, so the file can be read back exactly as it was written) ----
struct Variable
{
    string name;
    int32_t value;
};
struct Frame
{
    string func_name;
    int32_t argc;
    Variable argv[MAX_VARS_PER_FRAME];
    int32_t returnLine;
    Variable locals[MAX_VARS_PER_FRAME];
    int32_t localCount;
};
struct Snapshot
{
    Frame callStack[MAX_STACK_DEPTH];
    int32_t stackDepth;
};
struct TTDBHeader 
{
    char magic[4]; // "TTDB"
    int32_t version;
    int32_t stepCount;
    int64_t indexOffset;
};

// ---- Structs only the client needs ----
struct TdbgReader
{
    ssize_t file_index;
    TTDBHeader header;
    vector<int64_t> index;
};
struct DebugSession
{
    int active_flag;     // false until a "start" has worked
    TdbgReader reader;   // the .tdbg file stays open for the whole session
    int64_t currentStep; 
};

// =============================================================================================
// PART A: TALKING TO THE LOAD BALANCER
// =============================================================================================

bool recvAll(int sock, char *buf, size_t n)
{
    // recv() can return fewer bytes than asked for, so keep calling it until exactly n bytes are read.
     size_t rec = 0;
    while (rec < n)
    {
        ssize_t r = recv(sock, buf + rec, n - rec, 0);
        if (r < 0)
        {
            if (errno == EINTR)
                continue;               
            return false;
        }
        if (r == 0)
            return false;             
        rec += (size_t)r;
    }
    return true;
    // Return false if the connection closes or fails before n bytes arrive.
}
bool sendAll(int sock, const char *buf, size_t n)
{
    // send() can send fewer bytes than asked for, so keep calling it until all n bytes are sent.
      size_t sent = 0;
    while (sent < n)
    {
        ssize_t r = send(sock, buf + sent, n - sent, MSG_NOSIGNAL);
        if (r < 0)
        {
            if (errno == EINTR)
                continue;
            return false;
        }
        if (r == 0)
            return false;
        sent += (size_t)r;
    }
    return true;
    // Use the flag MSG_NOSIGNAL. Return false if the connection fails.
}

int connectToHost(const char *host, int port)
{
    // Create a TCP socket and connect it to host:port.
    // Return the socket, or -1 if the connection fails.
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
bool uploadSource(int sock, const char *sourcePath)
{
    // have to do checksum and then send
     FILE *f = fopen(sourcePath, "rb");
    if (f == nullptr)
        return false;
    uint32_t sum = 0;
    char status = 0;
    sum = checksumUpdate(sum, &status, 1);
    if (!sendAll(sock, &status, 1))
    {
        fclose(f);
        return false;
    }
    char buf[IO_BUFFER_SIZE];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
    {
        sum = checksumUpdate(sum, buf, n);
        if (!sendAll(sock, buf, n))
        {
            fclose(f);
            return false;
        }
    }
    bool readFailed = ferror(f) != 0;
    fclose(f);
    if (readFailed)
        return false;
    unsigned char t[4];
    encodeU32(sum, t);
    if (!sendAll(sock, (const char *)t, 4))
        return false;
    // When the file is finished, call shutdown(sock, SHUT_WR). This is how the server knows the string is complete.
    // Return false if the file cannot be opened or any send fails.
    shutdown(sock, SHUT_WR);
    return true;
}

bool receiveResponse(int sock, const char *outTdbgPath, string &outError)
{
     // Reply on the wire: [status][payload][CRC-32]. The CRC covers the status byte too.
    // Feed every received byte (status included) to a TrailerChecker.
    // Status 1: collect the payload into outError. Status 0: write the payload to the file.
    // At the end call verify(). If it fails, delete the file, set outError to
    // "checksum mismatch, response corrupted" and return false.
    TrailerChecker tc;
    char in[IO_BUFFER_SIZE];
    char body[IO_BUFFER_SIZE];

    bool anyData = false;
    bool gotStatus = false;
    int status = -1;
    FILE *f = nullptr;
    string errText;
    bool netFailed = false;
    bool writeFailed = false;

    while (true)
    {
        ssize_t r = recv(sock, in, sizeof(in), 0);
        if (r < 0)
        {
            if (errno == EINTR)
                continue;
            netFailed = true;
            break;
        }
        if (r == 0)
            break;                          
        anyData = true;

        size_t k = tc.feed(in, (size_t)r, body);   
        size_t pos = 0;

        // Pehla body byte status hai
        if (!gotStatus && k > 0)
        {
            status = (unsigned char)body[0];
            gotStatus = true;
            pos = 1;
            if (status == 0)
            {
                f = fopen(outTdbgPath, "wb");
                if (f == nullptr)
                {
                    writeFailed = true;
                    break;
                }
            }
        }

        if (k > pos)
        {
            if (status == 0)
            {
                if (fwrite(body + pos, 1, k - pos, f) != k - pos)
                {
                    writeFailed = true;
                    break;
                }
            }
            else
                errText.append(body + pos, k - pos);
        }
    }

    if (f != nullptr)
        fclose(f);

    if (writeFailed)
    {
        remove(outTdbgPath);
        outError = "could not write the .tdbg file";
        return false;
    }
    if (netFailed)
    {
        remove(outTdbgPath);
        outError = "connection to the load balancer was lost";
        return false;
    }
    if (!anyData)                          
    {
        outError = "no reply from the load balancer";
        return false;
    }
    if (!tc.verify())
    {
        remove(outTdbgPath);              
        outError = "checksum mismatch, response corrupted";
        return false;
    }

    if (status == 0)
        return true;                   

    outError = (status == 1) ? errText : "unknown reply status";
    remove(outTdbgPath);
    return false;
}

// =============================================================================================
// PART B: READING THE .tdbg FILE
// File layout (written by Phase 1):
//   header | snapshot 0 | snapshot 1 | ... | index (one 8 byte offset per snapshot)
// The index lets us jump straight to any step without reading the steps before it.
// =============================================================================================
static bool preadAll(int fd, void *buf, size_t n, int64_t offset)
{
    char *p = (char *)buf;
    size_t got = 0;
    while (got < n)
    {
        ssize_t r = pread(fd, p + got, n - got, (off_t)(offset + (int64_t)got));
        if (r < 0)
        {
            if (errno == EINTR)
                continue;
            return false;
        }
        if (r == 0)
            return false;         
        got += (size_t)r;
    }
    return true;
}
bool openTdbg(const char *path, TdbgReader &r)
{
    r.file_index = -1;
    r.index.clear();

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return false;

    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        close(fd);
        return false;
    }
    int64_t fileSize = (int64_t)st.st_size;
     unsigned char raw[TDBG_HEADER_SIZE];
    if (!preadAll(fd, raw, TDBG_HEADER_SIZE, 0))
    {
        close(fd);
        return false;
    }
    TTDBHeader h;
    memset(&h, 0, sizeof(h));
    memcpy(h.magic, raw, 4);
    memcpy(&h.version, raw + 4, 4);
    memcpy(&h.stepCount, raw + 8, 4);
    memcpy(&h.indexOffset, raw + 12, 8);
     if (memcmp(h.magic, "TTDB", 4) != 0 || h.stepCount <= 0 ||
        h.indexOffset < TDBG_HEADER_SIZE ||
        h.indexOffset + (int64_t)h.stepCount * 8 > fileSize)
    {
        close(fd);
        return false;
    }
    vector<int64_t> idx((size_t)h.stepCount);
    if (!preadAll(fd, idx.data(), idx.size() * sizeof(int64_t), h.indexOffset))
    {
        close(fd);
        return false;
    }
     for (size_t i = 0; i < idx.size(); i++)
    {
        if (idx[i] < TDBG_HEADER_SIZE || idx[i] >= h.indexOffset)
        {
            close(fd);
            return false;
        }
    }

    r.file_index = fd;
    r.header = h;
    r.index = idx;
    return true;
}
void closeTdbg(TdbgReader &r)
{
    // Free r.index and close the file.
    if (r.file_index >= 0)
        close((int)r.file_index);
    r.file_index = -1;
    vector<int64_t>().swap(r.index); 
}
Snapshot *readSnapshotAt(TdbgReader &r, int32_t step)
{
     if (r.file_index < 0 || step < 0 || step >= r.header.stepCount)
        return nullptr;
    
    int64_t start = r.index[step];
    int64_t end = (step + 1 < r.header.stepCount) ? r.index[step + 1] : r.header.indexOffset;
    if (end <= start)
        return nullptr;
    return nullptr;
}

// =============================================================================================
// PART C: THE SESSION IN MEMORY
// There is one DebugSession variable, created in main() and passed to every command by reference.
// =============================================================================================

void initSession(DebugSession &s)
{
   s.active_flag = 0;
   s.reader.file_index = -1;
   s.currentStep = 0;
   memset(&s.reader.header, 0, sizeof(s.reader.header));
   s.reader.index.clear();
}
void closeSession(DebugSession &s)
{
    // If active, call closeTdbg(s.reader). Then set active = false.
    if (s.active_flag)
    {
        closeTdbg(s.reader);
        s.active_flag = 0;
    }
    // Called when the user types quit and when the input ends.
}

// =============================================================================================
// PART D: THE COMMANDS
// =============================================================================================

void printSnapshot(int32_t step, int32_t totalSteps, const Snapshot &s); // defined in Part E

bool requestDebugFile(const string &host, int port, const char *sourcePath, const char* outTdbgPath, string &outError)
{
    FILE *f = fopen(sourcePath, "rb");
    if (f == nullptr)
    {
        outError = "could not open source file";
        return false;   
    }
    fclose(f);
    int sock = connectToHost(host.c_str(), port);
    if (sock < 0)
    {
        outError = "cannot connect to the load balancer";
        return false;
    }
    if (!uploadSource(sock, sourcePath))
    {
        close(sock);
        outError = "failed to send the source file to the load balancer";
        return false;
    }

    bool ok = receiveResponse(sock, outTdbgPath, outError);
    close(sock);
    return ok;
}

bool showCurrent(DebugSession &s)
{
    // Read the snapshot at s.currentStep, call printSnapshot, delete the snapshot.
    Snapshot *snapshot = readSnapshotAt(s.reader, s.currentStep);
    if (snapshot == nullptr)
    {
        printf("damaged .tdbg file\n");
        return false;
    }
     printSnapshot((int32_t)s.currentStep + 1, s.reader.header.stepCount, *snapshot);
    
    delete snapshot;
    return true;
}
bool moveBy(DebugSession &s, int32_t delta)
{
    if (!s.active_flag)
    {
        cout << "no session yet, use: start <source file>" << endl;
        return false;
    }

    int64_t target = s.currentStep + (int64_t)delta;   
    int32_t total = s.reader.header.stepCount;
    if (target < 0 || target >= total)
    {
        cout << "step out of range: you are at step " << (s.currentStep + 1)
             << " of " << total << " (valid steps: 1 to " << total << ")" << endl;
        return false;
    }
    
    int64_t old = s.currentStep;
    s.currentStep = target;
    if (!showCurrent(s))          
    {
        s.currentStep = old;
        return false;
    }
    return true;
}
bool cmdStart(DebugSession &s, const string &sourcePath)
{
    // 1. requestDebugFile with host and port, saving to TDBG_FILE.
     string tempPath = string(TDBG_FILE) + ".part";
     string err;
    if (!requestDebugFile(LB_HOST.c_str(), LB_PORT, sourcePath.c_str(), tempPath.c_str(), err))
    {
        cout << err << endl;
        return false;
    }
    // 2. If it fails, print the error text and return false. Keep the old session as it was.
    TdbgReader fresh;
    fresh.file_index = -1;
    if (!openTdbg(tempPath.c_str(), fresh))
    {
        remove(tempPath.c_str());
        cout << "server sent a damaged .tdbg file" << endl;
        return false;
    }
    // 3. Open the new file with openTdbg into a NEW local TdbgReader.
    //    If that fails, print "server sent a damaged .tdbg file", keep the old session, return false.
    Snapshot *first = readSnapshotAt(fresh, 0);
    if (first == nullptr)
    {
        closeTdbg(fresh);
        remove(tempPath.c_str());
        cout << "server sent a damaged .tdbg file" << endl;
        return false;
    }
    delete first;
    // 4. Only now close the old reader (if s.active), then put the new reader into s,
    //    set s.currentStep = 0 and s.active = true.
     if (rename(tempPath.c_str(), TDBG_FILE) != 0)
    {
        closeTdbg(fresh);
        remove(tempPath.c_str());
        cout << "could not save the .tdbg file" << endl;
        return false;
    }
      if (s.active_flag)
        closeTdbg(s.reader);
    s.reader = fresh;
    s.currentStep = 0;
    s.active_flag = 1;

    // 5. Show step 1 with showCurrent(s).
    return showCurrent(s);

}
bool cmdNext(DebugSession &s) {
    return moveBy(s, 1);
}
bool cmdPrv(DebugSession &s) {
    return moveBy(s, -1);
}
bool cmdJmpNext(DebugSession &s, int32_t n) {
    return moveBy(s, n);
}
bool cmdJmpPrv(DebugSession &s, int32_t n) {
    return moveBy(s, -n);
}

// =============================================================================================
// PART E: PRINTING
// =============================================================================================

static int32_t clampInt(int32_t v, int32_t lo, int32_t hi)
{
    if (v < lo)
        return lo;
    if (v > hi)
        return hi;
    return v;
}
void printSnapshot(int32_t step, int32_t totalSteps, const Snapshot &s)
{

     int32_t depth = clampInt(s.stackDepth, 0, MAX_STACK_DEPTH);
    cout << "==== Step " << step << " of " << totalSteps << " ====" << endl;
    if (depth == 0)
    {
        cout << "(empty call stack)" << endl << endl;
        return;
    }
    cout << "Call stack (innermost first):" << endl;
    for (int32_t i = depth - 1; i >= 0; i--)         
    {
        const Frame &f = s.callStack[i];

        int32_t argc = clampInt(f.argc, 0, MAX_VARS_PER_FRAME);
        cout << "  #" << i << " " << f.func_name << "(";
        for (int32_t a = 0; a < argc; a++)
        {
            if (a > 0)
                cout << ", ";
            cout << f.argv[a].name << " = " << f.argv[a].value;
        }
        cout << ")";
        if (i > 0)                                     
            cout << "   returns to line " << f.returnLine;
        cout << endl;

        int32_t n = clampInt(f.localCount, 0, MAX_VARS_PER_FRAME);
        if (n == 0)
            cout << "      locals: (none)" << endl;
        else
        {
            cout << "      locals: ";
            for (int32_t v = 0; v < n; v++)
            {
                if (v > 0)
                    cout << ", ";
                cout << f.locals[v].name << " = " << f.locals[v].value;
            }
            cout << endl;
        }
    }
    cout << endl;
}
void printHelp()
{
    // Print the commands
     cout << "Commands:" << endl;
    cout << "  start <source file>    send the file to the server and show step 1" << endl;
    cout << "  next                   go to the next step" << endl;
    cout << "  prv                    go to the previous step" << endl;
    cout << "  jmp next <n>           jump n steps forward" << endl;
    cout << "  jmp prv <n>            jump n steps backward" << endl;
    cout << "  help                   show this list" << endl;
    cout << "  quit / exit            leave the debugger" << endl;
    cout << "(you may also write \"" << PROGRAM_NAME << "\" before a command, e.g. \""
         << PROGRAM_NAME << " next\")" << endl;
}

// =============================================================================================
// PART F: THE INFINITE LOOP
// =============================================================================================
static vector<string> splitWords(const string &line)
{
    vector<string> words;
    istringstream in(line);
    string w;
    while (in >> w)
        words.push_back(w);
    return words;
}
static bool parsePositive(const string &t, int32_t &out)
{
    if (t.empty() || t.size() > 10)
        return false;
    long long v = 0;
    for (size_t i = 0; i < t.size(); i++)
    {
        if (t[i] < '0' || t[i] > '9')
            return false;
        v = v * 10 + (t[i] - '0');
    }
    if (v < 1 || v > MAX_JUMP)
        return false;
    out = (int32_t)v;
    return true;
}

bool runCommand(DebugSession &s, const string &line, bool &quitRequested)
{
    // 1. Split the line into words.
    vector<string> w = splitWords(line);
    if (w.empty())
        return false;    
    // 2. If the first word is "ttdb", drop it.
     if (w[0] == PROGRAM_NAME)
        w.erase(w.begin());
    // 3. Match the first word:
    //      start      -> needs a path after it, then cmdStart
    //      next       -> no more words, then cmdNext
    //      prv        -> no more words, then cmdPrv
    //      jmp        -> second word "next" or "prv", third word a positive number ,
    //                    then cmdJmpNext or cmdJmpPrv
    //      help       -> printHelp
    //      quit, exit -> set quitRequested = true
        if (!w.empty())
    {
        const string &cmd = w[0];

        if (cmd == "start" && w.size() == 2)
            return cmdStart(s, w[1]);

        if (cmd == "next" && w.size() == 1)
            return cmdNext(s);

        if (cmd == "prv" && w.size() == 1)
            return cmdPrv(s);

        if (cmd == "jmp" && w.size() == 3 && (w[1] == "next" || w[1] == "prv"))
        {
            int32_t n;
            if (!parsePositive(w[2], n))
            {
                cout << "jmp needs a whole number from 1 to " << MAX_JUMP << endl;
                return false;
            }
            return (w[1] == "next") ? cmdJmpNext(s, n) : cmdJmpPrv(s, n);
        }

        if (cmd == "help" && w.size() == 1)
        {
            printHelp();
            return true;
        }

        if ((cmd == "quit" || cmd == "exit") && w.size() == 1)
        {
            quitRequested = true;
            return true;
        }
    }

    // 4. For anything else, or a wrong number of words, print "unknown command" and the help text, and return false.
    cout << "unknown command" << endl;
    printHelp();
    return false;
}
int main(int argc, char *argv[])
{
    // you get string LB_HOST and int LB_PORT = 0 in arguments of this main function. 
     if (argc != 3)
    {
        cout << "usage: " << argv[0] << " <lb_ip> <lb_port>" << endl;
        return 1;
    }
    LB_HOST = argv[1];
    LB_PORT = atoi(argv[2]);
    if (LB_PORT < 1 || LB_PORT > 65535)
    {
        cout << "invalid port: " << argv[2] << endl;
        return 1;
    }

    // DebugSession session; initSession(session);
    DebugSession session; 
    initSession(session);
    // Print a short welcome and the help text.
     cout << "Welcome to the Time-Travel Debugger (" << PROGRAM_NAME << ")." << endl;
    printHelp();

    //
    // Infinite loop:
    //   if quitRequested, break
    //
    bool quitRequested = false;
    string line;
    while (true)
    {
        if (quitRequested)
            break;

        cout << PROMPT << flush;
        if (!getline(cin, line))            
        {
            cout << endl;
            break;
        }
        runCommand(session, line, quitRequested);   // koi bhi error loop nahi todta
    }
    // After the loop: closeSession(session) and return 0.
    // Never exit the loop because of an error. Only quit or the end of the input leaves it.

    closeSession(session);
    return 0;

}