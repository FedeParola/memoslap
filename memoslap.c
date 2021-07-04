#define _GNU_SOURCE

#include <argp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>

#define STORED_MSG "STORED\r\n"
#define BUFSIZE 2048
#define VALSIZE 1024
#define OP_LIST_SIZE 10
#define CACHE_LINE_SIZE 64
#define MAX_THREADS 8

enum operation {
    OP_GET,
    OP_STORE
};

enum handler_retcode {
    RET_NEED_READ,
    RET_NEED_WRITE
};

struct client {
    struct thread *thread;
    unsigned id;
    char buffer[BUFSIZE];
    int sockfd;
    unsigned current_op;
    unsigned n_read;
    unsigned to_read;
    unsigned n_written;
    unsigned to_write;
    unsigned seed;
};

struct thread {
    unsigned id;
    pthread_t thread;
    unsigned long n_operations;
} __attribute__((aligned(CACHE_LINE_SIZE)));

// Input variables
struct sockaddr_in server_addr = {
    .sin_family = AF_INET
};
static unsigned op_per_conn = 0;
static double get_share     = 0.9;
static unsigned nkeys       = 65536;
static unsigned nclients    = 128;
static unsigned nthreads    = 1;
static unsigned runtime     = 10;
static char no_fill         = 0;

// The sequence of operations each client should perform
enum operation op_list[OP_LIST_SIZE];
struct thread threads[MAX_THREADS];
volatile char stop;

// Args variables
const char *argp_program_version = "0.1";
const char *argp_program_bug_address = "<federico.parola@polito.it>";
static char doc[] = "memoslap -- load benchmark for memcached servers";
static char args_doc[] = "SERVER PORT";
static struct argp_option options[] = {
    {"clients",     'c', "NUM",      0, "Number of concurrent clients for every thread (default 128)"},
    {"get-share",   'g', "FRACTION", 0, "Share of get operations (default 0.9)"},
    {"keys",        'k', "NUM",      0, "Number of distinct keys to use (default 65536)"},
    {"threads",     't', "NUM",      0, "Number of threads (default 1)"},
    {"runtime",     'r', "SECS",     0, "Run time of the benchmark in seconds (default 10)"},
    {"op-per-conn", 'o', "NUM",      0, "Operations to execute for every TCP connection (default 0 = all operations in one connection)"},
    {"no-fill",     'n', 0,          0, "Do not fill the database before starting the benchmark"},
    { 0 }
};

static error_t
parse_opt(int key, char *arg, struct argp_state *state) {
    switch (key) {
    case 'c':
        nclients = atoi(arg);
        break;
    case 'g':
        get_share = atof(arg);
        if (get_share < 0 || get_share > 1) {
            fprintf(stderr, "Invalid get share\n");
            argp_usage(state);
        }
        break;
    case 'k':
        nkeys = atoi(arg);
        break;
    case 't':
        nthreads = atoi(arg);
        break;
    case 'r':
        runtime = atoi(arg);
        break;
    case 'o':
        op_per_conn = atoi(arg);
        break;
    case 'n':
        no_fill = 1;
        break;
    case ARGP_KEY_ARG:
        switch (state->arg_num) {
        case 0:
            if (!inet_aton(arg, &server_addr.sin_addr)) {
                fprintf(stderr, "Invalid server ip\n");
                argp_usage(state);
            }
            break;
        case 1:
            server_addr.sin_port = htons(atoi(arg));
            break;
        default:
            argp_usage(state);
        }
        break;
    case ARGP_KEY_END:
        if (state->arg_num < 2) {
            argp_usage(state);
        }
        break;
    default:
        return ARGP_ERR_UNKNOWN;
    }

    return 0;
}

static struct argp argp = { options, parse_opt, args_doc, doc };

// Prepare the next operation to execute for client c.
// Prepares the message of the operation (GET or SET) and optionally disconnects
// and reconnects if required.
void prepare_next_op(struct client *c) {
    c->current_op++;

    if (c->current_op == 1 ||
        (op_per_conn != 0 && c->current_op % op_per_conn == 0)) {
        // First connection or need to reconnect
        if (c->current_op > 1) {
            close(c->sockfd);
        }

        if ((c->sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0))
             == -1) {
            perror("Socket creation failed");
            exit(-1);
        }

        int res = connect(c->sockfd, (struct sockaddr *)&server_addr,
                          sizeof(struct sockaddr));
        if (res && errno != EINPROGRESS) {
            perror("Unable to connect to server");
            exit(-1);
        }
    
    }
    
    if (op_list[c->current_op % OP_LIST_SIZE] == OP_STORE) {
        sprintf(c->buffer, "set %d 0 0 %d\r\n", rand_r(&c->seed) % nkeys,
                VALSIZE);
        int len = strlen(c->buffer);

        // Leave whatever is there in the buffer as value
        len += VALSIZE + 2;
        c->buffer[len - 2] = '\r';
        c->buffer[len - 1] = '\n';

        c->to_write = len;
    
    } else if (op_list[c->current_op % OP_LIST_SIZE] == OP_GET) {
        sprintf(c->buffer, "get %d\r\n", rand_r(&c->seed) % nkeys);
        c->to_write = strlen(c->buffer);
    
    } else {
        fprintf(stderr, "Requested an invalid operation\n");
        exit(-1);
    }

    c->to_read = 0;
    c->n_read = 0;
    c->n_written = 0;
}

// Handle data ready to be read for client c.
// Might request another read if not all data are received or request the next
// operation (GET or SET).
// Returns the next operation to be performed on the socket (READ or WRITE).
enum handler_retcode handle_read(struct client *c) {
    int res;
    enum handler_retcode ret = RET_NEED_READ;

    res = recv(c->sockfd, c->buffer + c->n_read, BUFSIZE - c->n_read, 0);
    if (res == -1) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            return ret;
        } else {
            perror("Error receiving data");
            exit(-1);
        }
    }

    c->n_read += res;

    switch (op_list[c->current_op % OP_LIST_SIZE]) {
    case OP_GET:
        if (c->to_read == 0) {
            // Don't know the size of the response yet
            if (c->n_read > 0) {
                // The first character is enough to know if the operation was
                // successful
                if (c->buffer[0] != 'V') {
                    // This shouldn't happen, handle all cases as an error
                    c->buffer[c->n_read] = 0;
                    fprintf(stderr, "Error getting value. "
                            "Content of the buffer:\n'%s'\n", c->buffer);
                    exit(-1);
                }

                // The size of the value is delimited by the third space and \r
                int i;
                for (i = 0; i < c->n_read; i++) {
                    if (c->buffer[i] == '\r') break;
                }

                if (c->buffer[i] == '\r') {
                    int j;
                    for (j = i - 1; j > 0; j--) {
                        if (c->buffer[j] == ' ') break;
                    }

                    // Replace \r with \0 to terminate the size string
                    c->buffer[i] = 0;
                    long val_size = atoi(c->buffer + j + 1);

                    c->to_read = i + 2 + val_size + 2 + 5;
                }
            }
        }

        if (c->to_read > 0 && c->n_read >= c->to_read) {
            c->thread->n_operations++;
            prepare_next_op(c);
            ret = RET_NEED_WRITE;
        }

        break;
    
    case OP_STORE:
        if (c->to_read == 0) {
            // Don't know the size of the response yet
            if (c->n_read > 0) {
                // The first character is enough to know if the operation was
                // successful
                if (c->buffer[0] != 'S') {
                    // This shouldn't happen, handle all cases as an error
                    c->buffer[c->n_read] = 0;
                    fprintf(stderr, "Error storing value. "
                            "Content of the buffer:\n'%s'\n", c->buffer);
                    exit(-1);
                }

                c->to_read = strlen(STORED_MSG);
            }
        }

        if (c->to_read > 0 && c->n_read >= c->to_read) {
            c->thread->n_operations++;
            prepare_next_op(c);
            ret = RET_NEED_WRITE;
        }

        break;
    
    default:
        fprintf(stderr, "Unknown operation to perform\n");
        exit(-1);
    }

    return ret;
}

// Write the next part of the buffer for client c.
// Might request another write if not all buffer is written or schedule the
// reception of the response.
// Returns the next operation to be performed on the socket (READ or WRITE).
int handle_write(struct client *c) {
    int res;
    enum handler_retcode ret = RET_NEED_WRITE;

    res = write(c->sockfd, c->buffer + c->n_written,
                c->to_write - c->n_written);
    if (res == -1) {
        perror("Error writing data");
        exit(-1);
    }

    c->n_written += res;

    if (c->n_written == c->to_write) {
        return RET_NEED_READ;

    } else {
        return RET_NEED_WRITE;
    }
}

// Fill the target memcahched database with all available keys (content of
// values is not important).
void fill_database() {
    int sockfd, res;
    char buffer[BUFSIZE];
    unsigned to_write, written, to_read, read;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Socket creation failed");
        exit(-1);
    }

    if (connect(sockfd, (struct sockaddr *)&server_addr,
                sizeof(struct sockaddr))) {
        perror("Unable to connect to server");
        exit(-1);
    }

    for (int i = 0; i < nkeys; i++) {
        sprintf(buffer, "set %d 0 0 %d\r\n", i, VALSIZE);

        int len = strlen(buffer);

        // Leave whatever is there in the buffer as value
        len += VALSIZE + 2;
        buffer[len - 2] = '\r';
        buffer[len - 1] = '\n';

        written = 0;
        to_write = len;
        read = 0;
        to_read = 0;

        while (written < to_write) {
            res = write(sockfd, buffer + written, to_write - written);
            if (res == -1) {
                perror("Error writing data");
                exit(-1);
            }

            written += res;
        }

        while (read < to_read || to_read == 0) {
            res = recv(sockfd, buffer, BUFSIZE, 0);
            if (res == -1) {
                perror("Error receiving data");
                exit(-1);
            }

            read += res;

            if (to_read == 0 && read > 0) {
                // The first character is enough to know if the operation was
                // successful
                if (buffer[0] != 'S') {
                    // This shouldn't happen, handle all cases as an error
                    buffer[read] = 0;
                    fprintf(stderr, "Error storing value. "
                            "Content of the buffer:\n'%s'\n", buffer);
                    exit(-1);
                }

                to_read = strlen(STORED_MSG);
            }
        }
    }
}

// Run the benchamrk on a single thread.
void *run_benchmark(void *t) {
    struct thread *thread = t;
    int res, i, nfds, epollfd;
    struct client *clients, *c;
    struct epoll_event ev, *events;
    cpu_set_t cpuset;

    // Try to set affinity
    CPU_ZERO(&cpuset);
    CPU_SET(thread->id, &cpuset);
    if (pthread_setaffinity_np(thread->thread, sizeof(cpu_set_t), &cpuset)) {
        perror("Unable to set thread affinity");
    }

    epollfd = epoll_create1(0);
    if (epollfd == -1) {
        perror("Error creting epoll context");
        exit(-1);
    }

    clients = malloc(nclients * sizeof(struct client));
    events = malloc(nclients * sizeof(struct epoll_event));
    for (i = 0; i < nclients; i++) {
        clients[i].thread = thread;
        clients[i].id = i;
        clients[i].current_op = 0;
        clients[i].seed = i;
        prepare_next_op(&clients[i]);

        ev.events = EPOLLOUT;
        ev.data.ptr = &clients[i];
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, clients[i].sockfd, &ev) == -1) {
            perror("Error setting poll event");
            exit(-1);
        }
    }

    while (!stop) {
        nfds = epoll_wait(epollfd, events, nclients, -1);
        if (nfds == -1) {
            perror("Error polling the sockets");
            exit(-1);
        }
        if (nfds == 0) {
            fprintf(stderr, "epoll_wait() woke up due to timeout, "
                            "this should not happen\n");
            exit(-1);
        }

        for (i = 0; i < nfds; i++) {
            c = events[i].data.ptr;
            if (events[i].events & EPOLLIN) {
                res = handle_read(events[i].data.ptr);
                switch (res) {
                case RET_NEED_READ:
                    // Keep the same event
                    break;

                case RET_NEED_WRITE:
                    ev.events = EPOLLOUT;
                    ev.data.ptr = events[i].data.ptr;
                    if (epoll_ctl(epollfd, EPOLL_CTL_MOD, c->sockfd, &ev)
                            == -1) {
                        perror("Error setting poll event");
                        exit(-1);
                    }
                    break;

                default:
                    fprintf(stderr, "Unknown handler return code\n");
                    exit(-1);
                }

            } else if (events[i].events & EPOLLOUT) {
                res = handle_write(events[i].data.ptr);
                switch (res) {
                case RET_NEED_READ:
                    ev.events = EPOLLIN;
                    ev.data.ptr = events[i].data.ptr;
                    if (epoll_ctl(epollfd, EPOLL_CTL_MOD, c->sockfd, &ev)
                            == -1) {
                        perror("Error setting poll event");
                        exit(-1);
                    }
                    break;

                case RET_NEED_WRITE:
                    // Keep the same event
                    break;

                default:
                    fprintf(stderr, "Unknown handler return code\n");
                    exit(-1);
                }

            } else {
                fprintf(stderr, "Epoll woke up due to %04x on client %d\n",
                        events[i].events, i);
                exit(-1);
            }
        }
    }

    for (i = 0; i < nclients; i++) { 
        close(clients[i].sockfd);
    }

    free(events);
    free(clients);

    return NULL;
}

int main(int argc, char *argv[]) {
    int i;

    argp_parse(&argp, argc, argv, 0, 0, NULL);

    int ngets = get_share * OP_LIST_SIZE + 0.5;
    for (i = 0; i < ngets; i++) {
        op_list[i] = OP_GET;
    }
    for (; i < OP_LIST_SIZE; i++) {
        op_list[i] = OP_STORE;
    }

    if (!no_fill) {
        printf("Filling the database...\n");
        fill_database();
        printf("Database filled\n");
    }

    printf("Beginning benchmark\n");
    
    // Start threads
    stop = 0;
    for (i = 0; i < nthreads; i++) {
        threads[i].id = i;
        threads[i].n_operations = 0;

        if (pthread_create(&threads[i].thread, 0, run_benchmark, &threads[i])) {
            perror("Error creating thread");
            exit(-1);
        }
    }

    // Print stats every second
    int n_operations = 0, new_operations;
    unsigned left_runtime = runtime;
    for (; left_runtime > 0; left_runtime--) {
        sleep(1);

        new_operations = 0;
        for (i = 0; i < nthreads; i++) {
            new_operations += threads[i].n_operations;
        }

        printf("%3d: %d ops/s\n", runtime - left_runtime + 1, 
               (new_operations - n_operations));

        n_operations = new_operations;
    }

    stop = 1;

    // Join threads
    for (i = 0; i < nthreads; i++) {
        if (pthread_join(threads[i].thread, NULL)) {
            perror("Error joining thread");
            exit(-1);
        }
    }

    // Print stats
    n_operations = 0;
    for (i = 0; i < nthreads; i++) {
        n_operations += threads[i].n_operations;
    }
    printf("Avg: %d ops/s\n", n_operations / runtime);

    return 0;
}