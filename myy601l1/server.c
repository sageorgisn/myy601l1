/* server.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/


#include <signal.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"
#include <pthread.h>
#include <sys/time.h>
#include <stdio.h>


#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10

// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t notfull = PTHREAD_COND_INITIALIZER;
pthread_cond_t notempty = PTHREAD_COND_INITIALIZER;


struct stacknode{
    int fd; 
    struct timeval tv1; 
    struct timeval tv2; 
    struct timeval tv3; 
};

#define STACK_SIZE 4
#define THREAD_NUM 4
struct stacknode table[STACK_SIZE];    
int tablecount = 0;  


int completed_requests=0;
float total_waiting_time=0;
float total_service_time=0;



void append(int fd, struct timeval tv){
    while(tablecount==STACK_SIZE){
        pthread_cond_wait(&notfull, &mutex); 
    }
    table[tablecount].fd = fd;
    table[tablecount].tv1 = tv;
    tablecount++;
    pthread_cond_signal(&notempty);
}

struct stacknode take(){
    struct stacknode sn;
    
    while(tablecount==0){
        pthread_cond_wait(&notempty,&mutex);
    }
    sn = table[tablecount-1];
    tablecount--;
    pthread_cond_signal(&notfull);
    
    return sn;
}



// Definition of the database.
KISSDB *db = NULL;

/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;
  
  // Check arguments.
  if (!buffer)
    return NULL;
  
  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");    
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}

    
/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
void process_request(const int socket_fd) {
  char response_str[BUF_SIZE], request_str[BUF_SIZE];
    int numbytes = 0;
    Request *request = NULL;

    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);
    
    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);
    
    // parse the request.
    if (numbytes) {
      request = parse_request(request_str);
      if (request) {
        switch (request->operation) {
          case GET:
            // Read the given key from the database.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);
            break;
          case PUT:
            // Write the given key/value pair to the database.
            if (KISSDB_put(db, request->key, request->value)) 
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");
            break;
          default:
            // Unsupported operation.
            sprintf(response_str, "UNKOWN OPERATION\n");
        }
        // Reply to the client.
        write_str_to_socket(socket_fd, response_str, strlen(response_str));
        if (request)
          free(request);
        request = NULL;
        return;
      }
    }
    // Send an Error reply to the client.
    sprintf(response_str, "FORMAT ERROR\n");
    write_str_to_socket(socket_fd, response_str, strlen(response_str));
}


void onemoreRequest(struct stacknode node){
    float sec, sec2;
    float usec, usec2;
    
    sec  = node.tv2.tv_sec  - node.tv1.tv_sec;
    usec = node.tv2.tv_usec - node.tv1.tv_usec;
    
    sec2  = node.tv3.tv_sec  - node.tv2.tv_sec;
    usec2 = node.tv3.tv_usec - node.tv2.tv_usec;
    
    pthread_mutex_lock(&mutex2);
    completed_requests++;
    total_waiting_time = total_waiting_time + sec  + 0.000001 * usec;
    total_service_time = total_service_time + sec2 + 0.000001 * usec2;
    pthread_mutex_unlock(&mutex2);
    
    
}

void * myfunc(void *ptr){
    struct stacknode node;
    
    while(1){
        pthread_mutex_lock(&mutex);
        node = take();
        pthread_mutex_unlock(&mutex);
        
        gettimeofday(&(node.tv2), NULL); 
        process_request(node.fd);
        gettimeofday(&(node.tv3), NULL);
        
        onemoreRequest(node);   
        close(node.fd);
    }
}


void sigHandler(int signo){
    printf("signal SIGTSTP Received\n");
    printf("Program END!\n");
    
    printf("Completed: %d\n",completed_requests);
    if(completed_requests!=0){
        printf("Average Waiting: %.10f\n", total_waiting_time/completed_requests);
        printf("Average Service: %.10f\n", total_service_time/completed_requests);
    }
    exit(0);
    
}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {
    
  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information
                     
  pthread_t tid[THREAD_NUM];
  int res;
  int i;
  struct timeval tv;
  
 
                     
  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);
  
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);
  
  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }

  for(i=0; i<THREAD_NUM; i++){
    res = pthread_create(&(tid[i]), NULL, myfunc, NULL);
    if(res==-1){
        printf("Error Creating Thread: %d\n",i);
        exit(1);
    }
  }
    
  signal(SIGTSTP, sigHandler);
 
  // main loop: wait for new connection/requests
  while (1) { 
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }
    
    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));
    gettimeofday(&tv,NULL);

    pthread_mutex_lock(&mutex);
    append(new_fd, tv);
    pthread_mutex_unlock(&mutex);
    
    //process_request(new_fd);
    //close(new_fd);
  }  

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}

