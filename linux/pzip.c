#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/sysinfo.h>

#define CHUNK_SIZE 4096
#define QUEUE_SIZE 5
#define INIT_OUTPUT_BUFF_SIZE 20

typedef struct chunk_data {
    int id;
    int size;
    char *loc;
} chunk;

struct encoded_char_data {
    int num;
    char ch;
    struct encoded_char_data *next;
};
typedef struct encoded_char_data *encoded_char;

typedef struct proc_chunk_data {
    encoded_char head;
    encoded_char tail;
} proc_chunk;

void *producer(void *arg);
void *consumer(void *arg);
void do_fill(chunk *c);
chunk *do_get();
void encode_chunk(chunk *c);
char *map_open(char *fname, off_t *size);
void map_close(char *loc, off_t *size);
void exit_err(char *message);
void *Malloc(size_t size);
void Mutex_lock(pthread_mutex_t *mutex);
void Mutex_unlock(pthread_mutex_t *mutex);
void Cond_wait(pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex);
void Cond_signal(pthread_cond_t *cond);
void Pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine) (void *), void *arg);
void Pthread_join(pthread_t thread, void **retval);
encoded_char create_enc_char();
encoded_char add_enc_char(encoded_char tail, int num, char ch);


char **args;                // Global pointer to the program's arguments

chunk *buffer;              // Producer and consumer's shared chunk buffer
int fillptr = 0;            // Current index of producer in buffer
int useptr = 0;             // Current index of consumer in buffer
int numfull = 0;            // Number of elements in queue

int numFiles;               // Number of given files
int fileInd = 1;            // Current file index
int chunkID = 0;            // Unique identifier for current chunk
int chunkOffset = 0;        // Offset in bytes from current files starting addr
int fileDone = 0;           // Is the current file completely read?

char *fptr;                 // Pointer to the currently mapped file
off_t fsize;                // Size of the currently mapped file

proc_chunk *output_buff;    // Consumer's output buffer

pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t fill = PTHREAD_COND_INITIALIZER;

int main(int argc, char *argv[]) {
    // If no arguments are given, print usage
    if (argc == 1) {
        exit_err("file1 [file2 ...]");
    }

    args = argv;            // Store args in global variable
    numFiles = argc - 1;    // Get the number of files to be processed

    // Initialize the producer/consumer queue and output buffer
    buffer = (chunk *)Malloc(QUEUE_SIZE * sizeof(chunk));
    output_buff = (proc_chunk *)Malloc(INIT_OUTPUT_BUFF_SIZE * sizeof(proc_chunk));

    // Map the first file into mem
    fptr = map_open(argv[fileInd], &fsize);

    // Get processor info and calculate how many threads to use
    int cores = get_nprocs();
    int consumers = (cores == 1) ? (1) : (cores - 1);

    // Create threads
    pthread_t pid, cid[consumers];
    Pthread_create(&pid, NULL, producer, NULL);
    for (int i = 0; i < consumers; i++) {
        Pthread_create(&cid[i], NULL, consumer, NULL);
    }
    Pthread_join(pid, NULL);
    
    for (int i = 0; i < consumers; i++) {
        Pthread_join(cid[i], NULL);
    }

    // Free pointers
    free(buffer);
    free(output_buff);
    free(cid);
    
    exit(0);
}

void *producer(void *arg) {
    // Continue to produce until last file can no longer be chunked
    while (!(fileInd == numFiles && fileDone)) {

        // If the current file is done, unmap the old and map next file
        if (fileDone) {
            map_close(fptr, &fsize);
            fptr = map_open(args[fileInd], &fsize);
            fileInd++;
            fileDone = 0;
        }

        // Retrieve the next chunk, setting flags if EOF is reached
        chunk c;
        int comparison = fsize - (chunkOffset + CHUNK_SIZE);
        
        // Case 1: EOF isn't reached and a whole chunk can be taken
        if (comparison > 0) {
            chunk tmp = {chunkID, CHUNK_SIZE, fptr + chunkOffset};
            c = tmp;
            chunkID++;
            chunkOffset += CHUNK_SIZE;
        }
        // Case 2: EOF is reached and EXACTLY a full chunk can be taken
        else if (comparison == 0) {
            chunk tmp = {chunkID, CHUNK_SIZE, fptr + chunkOffset};
            c = tmp;
            chunkID++;
            chunkOffset = 0;    // File is done being chunked, reset offset
            fileDone = 1;       // Set flag to a map a new file
        }
        // Case 2: EOF is reached and less than full chunk can be taken
        else if (comparison < 0) {
            chunk tmp = {chunkID, fsize - chunkOffset, fptr + chunkOffset};
            c = tmp;
            chunkID++;
            chunkOffset = 0;    // File is done being chunked, reset offset
            fileDone = 1;       // Set flag to a map a new file
        }
        
        // Acquire lock and fill shared queue
        Mutex_lock(&m);
        while (numfull == QUEUE_SIZE) {
            Cond_wait(&empty, &m);
        }
        do_fill(&c);
        Cond_signal(&fill);
        Mutex_unlock(&m);
    }
    return NULL;
}

void *consumer(void *arg) {
    while (1) {
        // Acquire lock and get chunk from shared queue
        Mutex_lock(&m);
        while (numfull == 0) {
            Cond_wait(&fill, &m);
        }
        chunk *tmp = do_get();
        Cond_signal(&empty);
        Mutex_unlock(&m);

        // Process chunk
        encode_chunk(tmp);
    }
}

void do_fill(chunk *c) {
    // Fill relevant chunk data in shared queue
    buffer[fillptr].id = c->id;
    buffer[fillptr].size = c->size;
    buffer[fillptr].loc = c->loc;

    // Increment fillptr with wrap around
    fillptr = (fillptr + 1) % QUEUE_SIZE;

    // Update number of items in the queue
    numfull++;
}

chunk *do_get() {
    // Get relevant chunk data from shared queue
    chunk *tmp = &buffer[useptr];

    // Increment useptr with wrap around
    useptr = (useptr + 1) & QUEUE_SIZE;

    // Update number of items in the queue
    numfull--;

    // Return pointer to the retrieved chunk
    return tmp;
}

void encode_chunk(chunk *c) {
    // Make a pointer to chunk's position in the output hash table
    proc_chunk *output = &output_buff[c->id];

    output->head = create_enc_char();
    output->tail = output->head;

    char ch;
    char prev_ch = *(c->loc);
    int ch_count = 1;
    
    for (int i = 1; i < c->size; i++) {
        ch = *(c->loc + i);
        if (ch == prev_ch) {
            ch_count++;
        }
        else {
            output->tail = add_enc_char(output->tail, ch_count, prev_ch);
        }
        prev_ch = ch;
    }
    output->tail = add_enc_char(output->tail, ch_count, prev_ch);
}

encoded_char create_enc_char() {
    encoded_char ec = (encoded_char)Malloc(sizeof(struct encoded_char_data));
    ec->next = NULL;
    return ec;
}

encoded_char add_enc_char(encoded_char tail, int num, char ch) {
    encoded_char ec = create_enc_char();
    ec->num = num;
    ec->ch = ch;
    tail->next = ec;
    return ec;
}

/*******************************************************************************
 * Given a file name, maps the file into the virtual address space. Returns a 
 * char pointer to the starting address of the mapping. Records the size of the
 * file in the passed integer pointer.
 * 
 * Parameters:
 *      fname - the name of the file as a string
 *      size  - pointer to an int to store the file size
 * 
 * Returns:
 *      pointer to the starting address of the file in the VAS
 ******************************************************************************/
char *map_open(char *fname, off_t *size) {
    struct stat fs; // Holds file info
    char *loc;      // Pointer to addr zero of mapped file
    int fd;         // File descriptor for passed file

    // Get the file descriptor for the specified filename
    if ((fd = open(fname, O_RDONLY)) < 0) {
        exit_err("Couldn't open file.");
    }

    // Get the file's stats (namely size)
    if (fstat(fd, &fs) < 0) {
        exit_err("Call to fstat failed.");
    }

    // Make sure fd points to a regular file (e.g. not a directory)
    if (!S_ISREG(fs.st_mode)) {
        exit_err("Passed file is irregular.");
    }

    // Record file size in passed integer pointer
    *size = fs.st_size;

    // Attempt to map file
    loc = mmap(NULL, *size, PROT_READ, MAP_SHARED, fd, 0);
    if (loc == MAP_FAILED) {
        exit_err("Failed to map file.");
    }

    // Close the file descriptor
    if (close(fd) < 0) {
        exit_err("Failed to close file.");
    }

    // Return pointer to first address in the map
    return loc;
}

/*******************************************************************************
 * Unmaps a file from the virtual address space, given a pointer to its starting
 * address and its size.
 * 
 * Parameters:
 *      loc  - pointer to the starting address of the file in the VAS
 *      size - size of the file in bytes
 ******************************************************************************/
void map_close(char *loc, off_t *size) {
    if (munmap(loc, *size) < 0) {
        exit_err("Failed to unmap file.");
    }
}

/*******************************************************************************
 * Prints an error message to stdout, then exits with error code 1. 
 * Formatted as "pzip: <message>".
 * 
 * Parameters:
 *      message - the message to be printed out
 ******************************************************************************/
void exit_err(char *message) {
    printf("pzip: %s\n", message);
    exit(1);
}

/*******************************************************************************
 * A bunch of simple wrapper functions that exit when an error is detected.
 ******************************************************************************/
void *Malloc(size_t size) {
    void *ptr = malloc(size);
    if (ptr == NULL) {
        exit_err("Malloc failed.");
    }
    return ptr;
}

void Mutex_lock(pthread_mutex_t *mutex) {
    if (pthread_mutex_lock(mutex) != 0) {
        exit_err("Failed to acquire lock.");
    }
}

void Mutex_unlock(pthread_mutex_t *mutex) {
    if (pthread_mutex_unlock(mutex) != 0) {
        exit_err("Failed to release lock.");
    }
}

void Cond_wait(pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex) {
    if (pthread_cond_wait(cond, mutex) != 0) {
        exit_err("Failed to wait for condition signal.");
    }
}

void Cond_signal(pthread_cond_t *cond) {
    if (pthread_cond_signal(cond) != 0) {
        exit_err("Failed to send condition signal.");
    }
}

void Pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine) (void *), void *arg) {
    if (pthread_create(thread, attr, start_routine, arg) != 0) {
        exit_err("Failed to create new thread.");
    }
}

void Pthread_join(pthread_t thread, void **retval) {
    if (pthread_join(thread, retval) != 0) {
        exit_err("Failed to join threads.");
    }
}