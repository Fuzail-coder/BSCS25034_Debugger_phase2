// ======================= TIME-TRAVEL DEBUGGER - SERVER TEMPLATE =======================

// Pipeline this file implements, top to bottom:
//   0. Receive  -- stream the client's .trace bytes straight to source.bin on disk
//   1. Pass 0X0   -- validity check (FUNC/FUNC_END matching)
//   2. Pass 0X1   -- resolve(): copy EVERY source line into resolve.bin as [offset][size][string], then patch CALL targets.
//   3. Pass 0X2   -- execute resolve.bin: tokenize ONE line at a time, update the call stack, take a snapshot -> Timeline
//   4. Pass 0X3   -- serialize Timeline -> session.tdbg(header + snapshot records + dense index)


#include <iostream>
#include <string>
#include <cstdint>
#include <fstream>
#include <unistd.h>
#include <sys/socket.h>
#include <cstdint>
#include <cstdio>
  #include <cstring>
  #include <cerrno>
  #include <sstream>
  #include <signal.h>
  #include <sys/time.h>
  #include <netinet/in.h>
  #include <arpa/inet.h>
  #include "helper.h"
using namespace std;

// ---- Constants ----
#ifdef _WIN32
#define FSEEK64 _fseeki64
#define FTELL64 _ftelli64
#else
#define FSEEK64 fseeko
#define FTELL64 ftello
#endif

const int32_t MAX_VARS_PER_FRAME = 16;
const int32_t MAX_STACK_DEPTH = 64;
const int32_t MAX_FUNCS = 128;
const int32_t MAX_TOKENS = MAX_VARS_PER_FRAME + 2; // kW + func_name + upto 16 params/args
const int32_t MAX_PATCHES = MAX_FUNCS * 4;
const uint64_t MAX_SOURCE_BYTES = 15ULL * 1024 * 1024; // sanity cap on the declared file length
const int32_t IO_BUFFER_SIZE = 64 * 1024;                  // fixed buffer for streaming to/from disk
const int32_t SOCKET_TIMEOUT_SEC = 5;      
const char* const FMODE_READ = "rb";
const char* const FMODE_WRITE = "wb";
const char* const FMODE_UPDATE = "r+b";
const char* const FMODE_CREATE_UPDATE = "w+b";                // TODO: apply as SO_RCVTIMEO so a deadclient can't hang the server forever

// ---- Custom data structures
inline void stripTrailingCR(string& s)
{
    if (!s.empty() and s.back() == '\r')
        s.pop_back();
}
struct NetworkInit
{
    bool ok;
    NetworkInit() : ok(true)
    {
#ifdef _WIN32
        WSADATA wsa;
        ok = (WSAStartup(MAKEWORD(2, 2), &wsa) == 0);
#endif
    }
    ~NetworkInit()
    {
#ifdef _WIN32
        if (ok)
            WSACleanup();
#endif
    }
};

// Stack: back the live Call Stack during execution
template <typename T>
class Stack
{
    struct Node
    {
        T data;
        Node* next;
    };
    Node* top;
    int32_t count;

public:
    // Implement these functions:
    Stack()
    { // initialize the stack
        count = 0;
        top = nullptr;
    }

    void push(const T& val)
    {

        // pushes the value on the stack if max limit is not reached yet.
        if (count >= MAX_STACK_DEPTH)
            return;
        top = new Node{ val, top };
        count++;
    }
    T pop()
    {
        // pop the top value on the stack
        Node* temp = top;
        T val = temp->data;
        top = top->next;

        delete temp;
        count--;
        return val;
    }
    T& peek()
    {
        // returns the top value on the stack
        return top->data;
    }
    bool isEmpty()
    {
        return top == nullptr;
    }
    int32_t depth()
    {
        return count;
    }
    int32_t snapshot_into(T out[], int32_t maxLen)
    {
        // copies every frame, top to bottom in the array given as a parameter
        // this is what buildSnapshot() call, returns count written
        int32_t written = 0;
        Node* ptr = top;
        while (ptr != nullptr and written < maxLen)
        {
            out[written] = ptr->data;
            written++;
            ptr = ptr->next;
        }
        return written;
    }
};


// Timeline : doubly linked list of Snapshots
struct Snapshot; // fwd declaration;
struct TimelineNode
{
    Snapshot *data;
    TimelineNode *next;
    TimelineNode *prev;
};
class Timeline
{
    TimelineNode *head, *tail;
    int32_t stepCount;

public:
    // Implement these functions
    Timeline()
    {
        head = tail = nullptr;
        stepCount = 0;
    }
    void record(Snapshot* s)
    {
        // add record in the timeline
        TimelineNode* n = new TimelineNode{ s, nullptr, tail };
        stepCount += 1;
        if (!tail)
        {
            head = n;
        }
        else
        {
            tail->next = n;
        }
        tail = n;
    }
    TimelineNode* begin()
    {
        return head;
    }
    int32_t getStepCount()
    {
        return stepCount;
    }
};

// Core structs
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




// PASS 0x0: READING source.bin + VALIDITY CHECK
static int32_t WordsCounter(const string& line)
{
    int32_t words = 0;
    for (size_t i = 0; i < line.size();)
    {
        while (i < line.size() and line[i] == ' ')
        {
            i++;
        }
        if (i >= line.size())
            break;
        words++;
        while (i < line.size() and line[i] != ' ')
        {
            i++;
        }
    }
    return words;
}

static string wordAt(const string& line, int32_t index)
{
    int32_t words = 0;
    for (size_t i = 0; i < line.size();)
    {
        while (i < line.size() and line[i] == ' ')
        {
            i++;
        }
        if (i >= line.size())
            break;

        size_t start = i;
        while (i < line.size() and line[i] != ' ')
        {
            i++;
        }

        if (words == index)
            return line.substr(start, i - start);
        words++;
    }
    return "";
}
string firstWord(const string& line)
{
    // returns first word from the input string
    string w = wordAt(line, 0);
    for (size_t i = 0; i < w.size(); i++)
    {
        w[i] = tolower(w[i]);
    }
    return w;
}
string secondWord(const string& line)
{
    // returns the second word
    return wordAt(line, 1);
}
bool readSourceLine(ifstream& in, string& out)
{
    // reads the next nonblank line
    string line;
    while (getline(in, line))
    {
        stripTrailingCR(line);

        size_t first = 0;
        while (first < line.size() and (line[first] == ' ' or line[first] == '\t'))
            first++;

        if (first == line.size())
            continue;

        size_t last = line.size() - 1;
        while (line[last] == ' ' or line[last] == '\t')
            last--;

        line = line.substr(first, last - first + 1);

        if (line.size() >= 2 and line[0] == '/' and line[1] == '/')
            continue;

        out = line;
        return true;
    }
    return false;
}

bool validateProgram(const char* sourcePath)
{
    // for each func defined there should be exactly one func_end and no nested funcs allowed - 
    ifstream fin(sourcePath, ios::binary);
    if (!fin)
    {
        cout << "File Not Found!....." << endl;
        return false;
    }

    string line;
    bool insideFunc = false;
    int32_t lineNo = 0;

    while (readSourceLine(fin, line))
    {
        lineNo++;
        string parameter = firstWord(line);
        int32_t words = WordsCounter(line);

        if (parameter == "func")
        {
            if (insideFunc)
            {
                cout << "Line " << lineNo << ": Nested Function! Error" << endl;
                return false;
            }
            if (secondWord(line).empty())
            {
                cout << "Line " << lineNo << ": Function name missing" << endl;
                return false;
            }
            insideFunc = true;
        }
        else if (parameter == "func_end")
        {
            if (!insideFunc)
            {
                cout << "Line " << lineNo << ": func_end without func" << endl;
                return false;
            }
            if (words != 1)
            {
                cout << "Line " << lineNo << ": func_end takes no arguments" << endl;
                return false;
            }
            insideFunc = false;
        }
        else if (parameter == "call" or parameter == "set" or parameter == "add" or
            parameter == "sub" or parameter == "mul" or parameter == "div")
        {
            if (!insideFunc)
            {
                cout << "Line " << lineNo << ": Instruction outside a function" << endl;
                return false;
            }
            int32_t needed = (parameter == "call") ? 2 : 3;
            if (words < needed)
            {
                cout << "Line " << lineNo << ": Missing operands" << endl;
                return false;
            }
        }
        else
        {
            cout << "Line " << lineNo << ": Operator doesn't exist" << endl;
            return false;
        }

        if (words - 2 > MAX_VARS_PER_FRAME)
        {
            cout << "Line " << lineNo << ": More Variables" << endl;
            return false;
        }
    }

    if (insideFunc)
    {
        cout << "Missing func_end" << endl;
        return false;
    }
    return true;
}
// PASS 0x1: RESOLVE() -> resolve.bin
int64_t writeResolveRecord(FILE* f, int64_t offsetField, const string& text)
{
    // writes one [offset(8B)][size(4B)][string] record at the current file position
    // returns this record's own starting byte position
    int64_t startPos = FTELL64(f);
    int32_t size = static_cast<int32_t>(text.size());

    fwrite(&offsetField, sizeof(int64_t), 1, f);
    fwrite(&size, sizeof(int32_t), 1, f);
    fwrite(text.data(), 1, size, f);

    return startPos;
}

int64_t readResolveRecord(FILE* f, string& outText)
{
    // reads one record at the current position and advances past it, returns the offset field - the raw line text comes back untouched in outText.
    int64_t offsetField;
    int32_t size;

    if (fread(&offsetField, sizeof(int64_t), 1, f) != 1)
        return -1;
    if (fread(&size, sizeof(int32_t), 1, f) != 1)
        return -1;

    outText.resize(size);
    if (size > 0 and fread(&outText[0], 1, size, f) != static_cast<size_t>(size))
        return -1;

    return offsetField;
}
int64_t resolveProgram(const char* sourcePath, const char* resolveBinPath)
{
    FuncEntry funcArray[MAX_FUNCS];
    int32_t funcCount = 0;
    PendingPatch patches[MAX_PATCHES];
    int32_t patchCount = 0;
    // Every source line becomes one record holding the raw line, as-is.
    // resolve() only PEEKS at the leading word(s) -- enough to spot FUNC
    // (remember its position) and CALL (remember which function it needs
    // and where its offset field sits).
    // Once the whole file is written, every CALL's offset field is patched
    // with its target's position. Patching happens after the full write
    // Returns the byte offset of main's FUNC header record.
    // if there is no main return the error 
    ifstream fin(sourcePath, ios::binary);
    if (!fin)
    {
        cout << "Source file not found" << endl;
        return -1;
    }
    FILE* out = fopen(resolveBinPath, FMODE_CREATE_UPDATE);

    if (!out)
    {
        cout << "Cannot create resolve.bin" << endl;
        return -1;
    }

    string line;
    while (readSourceLine(fin, line))
    {
        string kw = firstWord(line);
        int64_t pos = writeResolveRecord(out, 0, line);
        if (kw == "func")
        {
            if (funcCount >= MAX_FUNCS)
            {
                cout << "Too many functions" << endl;
                fclose(out);
                return -1;
            }
            string name = secondWord(line);
            for (int32_t i = 0; i < funcCount; i++)
            {
                if (funcArray[i].funcName == name)
                {
                    cout << "Duplicate function: " << name << endl;
                    fclose(out);
                    return -1;
                }
            }
            funcArray[funcCount].funcName = name;
            funcArray[funcCount].byteOffsetInResolveBin = pos;
            funcCount++;
        }
        else if (kw == "call")
        {
            if (patchCount >= MAX_PATCHES)
            {
                cout << "Too many calls" << endl;
                fclose(out);
                return -1;
            }
            patches[patchCount].byteOffsetOfOffsetField = pos;
            patches[patchCount].targetFuncName = secondWord(line);
            patchCount++;
        }
    }

    for (int32_t i = 0; i < patchCount; i++)
    {
        int64_t target = -1;
        for (int32_t j = 0; j < funcCount; j++)
        {
            if (funcArray[j].funcName == patches[i].targetFuncName)
            {
                target = funcArray[j].byteOffsetInResolveBin;
                break;
            }
        }

        if (target == -1)
        {
            cout << "Undefined function: " << patches[i].targetFuncName << endl;
            fclose(out);
            return -1;
        }

        FSEEK64(out, patches[i].byteOffsetOfOffsetField, SEEK_SET);
        fwrite(&target, sizeof(int64_t), 1, out);
    }

    int64_t mainOffset = -1;
    for (int32_t i = 0; i < funcCount; i++)
    {
        if (funcArray[i].funcName == "main")
        {
            mainOffset = funcArray[i].byteOffsetInResolveBin;
            break;
        }
    }

    fclose(out);

    if (mainOffset == -1)
    {
        cout << "No main function found" << endl;
        return -1;
    }
    return mainOffset;
}

// PASS 0x2: EXECUTION (tokenization happens here)
enum TokenKind
{
    KEYWORD,
    IDENTIFIER,
    PARAM
};
struct Token
{
    TokenKind type;
    string text;
};
int32_t tokenizeLine(const string& line, Token tokens[], int32_t maxTokens)
{
    // first word is always a instruction keyword
    // next word is identifier (func name / variable name)
    // after identifier all are params/args, space separated
    int32_t count = 0;
    size_t i = 0;

    while (i < line.size() and count < maxTokens)
    {
        while (i < line.size() and line[i] == ' ')
            i++;
        if (i >= line.size())
            break;

        size_t start = i;
        while (i < line.size() and line[i] != ' ')
            i++;
        string word = line.substr(start, i - start);

        if (count == 0)
        {
            for (size_t k = 0; k < word.size(); k++)
                word[k] = tolower(word[k]);
            tokens[count].type = KEYWORD;
        }
        else if (count == 1)
            tokens[count].type = IDENTIFIER;
        else
            tokens[count].type = PARAM;

        tokens[count].text = word;
        count++;
    }
    return count;
}
Snapshot* buildSnapshot(Stack<Frame>& callStack)
{
    // build the snapshot based on the callStack given
    Snapshot* s = new Snapshot();
    s->stackDepth = callStack.snapshot_into(s->callStack, MAX_STACK_DEPTH);
    return s;
}
bool isNumber(const string& s)
{
    if (s.empty())
        return false;
    size_t i = 0;
    if (s[0] == '-' or s[0] == '+')
        i = 1;
    if (i >= s.size())
        return false;
    for (; i < s.size(); i++)
        if (!isdigit(static_cast<unsigned char>(s[i])))
            return false;
    return true;
}

Variable* findVar(Frame& fr, const string& name)
{
    for (int32_t i = 0; i < fr.argc; i++)
        if (fr.argv[i].name == name)
            return &fr.argv[i];
    for (int32_t i = 0; i < fr.localCount; i++)
        if (fr.locals[i].name == name)
            return &fr.locals[i];
    return nullptr;
}

bool getValue(Frame& fr, const string& token, int32_t& out)
{
    if (isNumber(token))
    {
        out = static_cast<int32_t>(strtol(token.c_str(), nullptr, 10));
        return true;
    }
    Variable* v = findVar(fr, token);
    if (v == nullptr)
        return false;
    out = v->value;
    return true;
}

bool setVar(Frame& fr, const string& name, int32_t value)
{
    Variable* v = findVar(fr, name);
    if (v != nullptr)
    {
        v->value = value;
        return true;
    }
    if (fr.localCount >= MAX_VARS_PER_FRAME)
        return false;
    fr.locals[fr.localCount].name = name;
    fr.locals[fr.localCount].value = value;
    fr.localCount++;
    return true;
}

int32_t recordIndexAt(FILE* f, int64_t targetOffset)
{
    int64_t saved = FTELL64(f);
    FSEEK64(f, 0, SEEK_SET);
    string tmp;
    int32_t index = 0;
    while (FTELL64(f) < targetOffset)
    {
        if (readResolveRecord(f, tmp) == -1)
            break;
        index++;
    }
    FSEEK64(f, saved, SEEK_SET);
    return index;
}

bool seekToRecord(FILE* f, int32_t index)
{
    FSEEK64(f, 0, SEEK_SET);
    string tmp;
    for (int32_t i = 0; i < index; i++)
        if (readResolveRecord(f, tmp) == -1)
            return false;
    return true;
} 

void executeProgram(const char* resolveBinPath, int64_t mainOffset, Timeline& timeline)
{
    // initialize the call stack
    // make the main frame
    // push main frame on the call stack

    // implementation:
    // execute line by line, and according to the keyword perform action
    if (mainOffset < 0)
        return;

    FILE* f = fopen(resolveBinPath, FMODE_READ);
    if (!f)
    {
        cout << "Cannot open resolve.bin" << endl;
        return;
    }

    Stack<Frame> callStack;
    Token tokens[MAX_TOKENS];
    string line;

    FSEEK64(f, mainOffset, SEEK_SET);
    if (readResolveRecord(f, line) == -1)
    {
        cout << "Cannot read main" << endl;
        fclose(f);
        return;
    }
    tokenizeLine(line, tokens, MAX_TOKENS);

    Frame mainFrame;
    mainFrame.func_name = tokens[1].text;
    mainFrame.argc = 0;
    mainFrame.returnLine = -1;
    mainFrame.localCount = 0;
    callStack.push(mainFrame);

    bool ok = true;
    while (ok and !callStack.isEmpty())
    {
        int64_t targetOffset = readResolveRecord(f, line); 
        if (targetOffset == -1)
        {
            cout << "Unexpected end of program" << endl;
            break;
        }

        int32_t n = tokenizeLine(line, tokens, MAX_TOKENS);
        string kw = tokens[0].text;
        Frame& cur = callStack.peek();

        if (kw == "set")
        {
            int32_t v;
            if (n < 3 or !getValue(cur, tokens[2].text, v))
            {
                cout << "Runtime error: bad operand in '" << line << "'" << endl;
                ok = false;
            }
            else if (!setVar(cur, tokens[1].text, v))
            {
                cout << "Runtime error: too many variables in " << cur.func_name << endl;
                ok = false;
            }
        }
        else if (kw == "add" or kw == "sub" or kw == "mul" or kw == "div")
        {
            Variable* target = findVar(cur, tokens[1].text);
            int32_t rhs;
            if (n < 3 or target == nullptr or !getValue(cur, tokens[2].text, rhs))
            {
                cout << "Runtime error: undefined variable in '" << line << "'" << endl;
                ok = false;
            }
            else if (kw == "add")
                target->value += rhs;
            else if (kw == "sub")
                target->value -= rhs;
            else if (kw == "mul")
                target->value *= rhs;
            else
            {
                if (rhs == 0)
                {
                    cout << "Runtime error: division by zero in '" << line << "'" << endl;
                    ok = false;
                }
                else
                    target->value /= rhs;
            }
        }
        else if (kw == "call")
        {
            if (callStack.depth() >= MAX_STACK_DEPTH)
            {
                cout << "Runtime error: stack overflow (call " << tokens[1].text << ")" << endl;
                ok = false;
            }
            else
            {
                int32_t returnIndex = recordIndexAt(f, FTELL64(f));

                int32_t argCount = n - 2;
                Token callee[MAX_TOKENS];
                string hdr;

                FSEEK64(f, targetOffset, SEEK_SET); 
                readResolveRecord(f, hdr);
                int32_t hn = tokenizeLine(hdr, callee, MAX_TOKENS);
                int32_t paramCount = hn - 2;

                if (argCount != paramCount)
                {
                    cout << "Runtime error: " << callee[1].text << " expects " << paramCount
                        << " args, got " << argCount << endl;
                    ok = false;
                }
                else
                {
                    Frame fr;
                    fr.func_name = callee[1].text;
                    fr.argc = paramCount;
                    fr.returnLine = returnIndex;
                    fr.localCount = 0;

                    for (int32_t i = 0; i < paramCount and ok; i++)
                    {
                        int32_t v;
                        if (!getValue(cur, tokens[2 + i].text, v))
                        {
                            cout << "Runtime error: undefined argument '" << tokens[2 + i].text << "'" << endl;
                            ok = false;
                            break;
                        }
                        fr.argv[i].name = callee[2 + i].text;
                        fr.argv[i].value = v;
                    }
                    if (ok)
                        callStack.push(fr);
                }
            }
        }
        else if (kw == "func_end")
        {
            Frame done = callStack.pop();

            if (!callStack.isEmpty())
            {
                seekToRecord(f, done.returnLine - 1); 
                string callLine;
                readResolveRecord(f, callLine);
                Token ct[MAX_TOKENS];
                tokenizeLine(callLine, ct, MAX_TOKENS);

                Frame& caller = callStack.peek();
                for (int32_t i = 0; i < done.argc; i++)
                {
                    const string& argName = ct[2 + i].text;
                    if (isNumber(argName))
                        continue; 
                    Variable* v = findVar(caller, argName);
                    if (v != nullptr)
                        v->value = done.argv[i].value;
                }

                seekToRecord(f, done.returnLine); 
            }
        }
        else
        {
            cout << "Runtime error: unknown instruction '" << kw << "'" << endl;
            ok = false;
        }

        if (ok)
            timeline.record(buildSnapshot(callStack)); 
    }

    fclose(f);
    while (!callStack.isEmpty())
        callStack.pop();
}
// PASS 0x3: SERIALIZE TIMELINE

void writeHeader(FILE* f, const TTDBHeader& h)
{
    fwrite(h.magic, 1, 4, f);
    fwrite(&h.version, sizeof(int32_t), 1, f);
    fwrite(&h.stepCount, sizeof(int32_t), 1, f);
    fwrite(&h.indexOffset, sizeof(int64_t), 1, f);
}

static void writeString(FILE* f, const string& s)
{
    int32_t len = static_cast<int32_t>(s.size());
    fwrite(&len, sizeof(int32_t), 1, f);
    if (len > 0)
        fwrite(s.data(), 1, len, f);
}

static void writeVariable(FILE* f, const Variable& v)
{
    writeString(f, v.name);
    fwrite(&v.value, sizeof(int32_t), 1, f);
}

static void writeFrame(FILE* f, const Frame& fr)
{
    writeString(f, fr.func_name);

    fwrite(&fr.argc, sizeof(int32_t), 1, f);
    for (int32_t i = 0; i < fr.argc; i++)
        writeVariable(f, fr.argv[i]);

    fwrite(&fr.returnLine, sizeof(int32_t), 1, f);

    fwrite(&fr.localCount, sizeof(int32_t), 1, f);
    for (int32_t i = 0; i < fr.localCount; i++)
        writeVariable(f, fr.locals[i]);
}

static void writeSnapshot(FILE* f, const Snapshot& s)
{
    fwrite(&s.stackDepth, sizeof(int32_t), 1, f);
    for (int32_t i = 0; i < s.stackDepth; i++)
        writeFrame(f, s.callStack[i]);
}

void writeTdbg(Timeline& timeline, const char* tdbgPath)
{
    // placeholder for header
    // index array of the size of stepcount from the timeline
    // placing each snapshot in the file while maintaining the index(starting point of each nth snapshot)
    // after timeline add the index array i the file
    // update the header
    FILE* f = fopen(tdbgPath, FMODE_CREATE_UPDATE);
    if (!f)
    {
        cout << "Cannot create " << tdbgPath << endl;
        return;
    }

    int32_t steps = timeline.getStepCount();

    TTDBHeader h;
    h.magic[0] = 'T';
    h.magic[1] = 'T';
    h.magic[2] = 'D';
    h.magic[3] = 'B';
    h.version = 1;
    h.stepCount = steps;
    h.indexOffset = 0;
    writeHeader(f, h);

    int64_t* index = new int64_t[steps > 0 ? steps : 1];

    TimelineNode* node = timeline.begin();
    int32_t i = 0;
    while (node != nullptr and i < steps)
    {
        index[i] = FTELL64(f);
        writeSnapshot(f, *node->data);
        node = node->next;
        i++;
    }

    h.indexOffset = FTELL64(f);
    if (steps > 0)
        fwrite(index, sizeof(int64_t), steps, f);

    FSEEK64(f, 0, SEEK_SET);
    writeHeader(f, h);

    fclose(f);
    delete[] index;
}


// main section
int main()
{
    NetworkInit net;
    if (!net.ok)
    {
        cerr << "WSAStartup failed\n";
        return 1;
    }

    if (!validateProgram("source.bin"))
    {
        // send an error response instead of a .tdbg file
        return 1;
    }

    int64_t mainOffset = resolveProgram("source.bin", "resolve.bin");

    Timeline timeline;
    executeProgram("resolve.bin", mainOffset, timeline);
    writeTdbg(timeline, "session.tdbg");

    return 0;
}   