#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/sysinfo.h>

#define CHUNK_SIZE 4096         // Default chunk size
#define QUEUE_SIZE 10           // Size of shared prod-consumer buffer
#define INIT_OUT_BUFFSIZE 16    // Initial size of output buffer

// Holds the relevant info for an unprocessed input chunk
typedef struct unproc_chunk_data {
    int id;                     // Chunk's unique ID
    int size;                   // Chunk's size
    char *loc;                  // Chunks starting mem addr
} unproc_chunk;

struct encoded_char {
    int num;
    char ch;
} __attribute__((packed));
typedef struct encoded_char enc_ch;

typedef struct proc_chunk_data {
    int numElements;
    int size;
    enc_ch *buffer;
} proc_chunk;

char get_last_ch(int chunkID);
char get_first_ch(int chunkID);
void set_first_num(int chunkID, int n);
int get_first_num(int chunkID);
int get_last_num(int chunkID);
unproc_chunk *do_get();
void do_fill(unproc_chunk *c);
void *consumer(void *arg);
void *producer(void *arg);
void encode_chunk(unproc_chunk *c);
void add_enc_ch(proc_chunk *output, int ch_count, char prev_ch);
char *map_open(char *fname, int *size);
void map_close(char *loc, int *size);
void exit_err(char *message);

void *Malloc(size_t size);
void *Realloc(void *ptr, size_t size);
void Mutex_lock(pthread_mutex_t *mutex);
void Mutex_unlock(pthread_mutex_t *mutex);
void Cond_wait(pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex);
void Cond_signal(pthread_cond_t *cond);
void Cond_broadcast(pthread_cond_t *cond);
void Pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine) (void *), void *arg);
void Pthread_join(pthread_t thread, void **retval);

int numFiles;                   // Number of given files
char **fptrs;                   // Pointer to the currently mapped file
int *fsizes;                    // Size of the currently mapped file
int numChunks;                  // total number of chunks

unproc_chunk *chunkBuff;        // Producer and consumer's shared buffer
int fillptr = 0;                // Current index of producer in buffer
int useptr = 0;                 // Current index of consumer in buffer
int numfull = 0;                // Number of elements in shared buffer
int done = 0;                   // Flag that lets consumers know producer has finished

proc_chunk *outBuff;            // A hashtable where encoded chunks are stored

// Declare and initialize locks and CV's
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t fill = PTHREAD_COND_INITIALIZER;

int main(int argc, char *argv[]) {
    // If no arguments are given, print usage
    if (argc == 1) {
        exit_err("file1 [file2 ...]");
    }

    // Get the number of files to be processed
    numFiles = argc - 1;    

    // Map all files into mem
    int totalSize = 0;
    fptrs = (char **)Malloc(numFiles * sizeof(char *));
    fsizes = (int *)Malloc(numFiles * sizeof(int));
    for (int i = 0; i < numFiles; i++) {
        fptrs[i] = map_open(argv[i + 1], &fsizes[i]);
        totalSize += fsizes[i];
    }

    // Initialize producer-consumer shared buffer and output buffer
    chunkBuff = (unproc_chunk *)Malloc(QUEUE_SIZE * sizeof(unproc_chunk));
    int outBuffSize = (totalSize / CHUNK_SIZE) + 1;
    outBuff = (proc_chunk *)Malloc(outBuffSize * sizeof(proc_chunk));

    // No. of consumers = total cores - 1 (reserve 1 core for producer)
    int consumers = get_nprocs();
    if (consumers > 1) {
        consumers--;
    }

    // Create threads
    pthread_t pid, cid[consumers];
    Pthread_create(&pid, NULL, producer, NULL);
    for (int i = 0; i < consumers; i++) {
        Pthread_create(&cid[i], NULL, consumer, NULL);
    }

    // Join threads
    Pthread_join(pid, NULL);
    for (int i = 0; i < consumers; i++) {
        Pthread_join(cid[i], NULL);
    }

    // Stitch files back together
    int length = 0;
    for (int i = 0; i < numChunks - 1; i++) {
        length = outBuff[i].numElements;
        if (get_last_ch(i) == get_first_ch(i + 1)) {
            set_first_num(i + 1, get_first_num(i + 1) + get_last_num(i));
            if (length > 1) {
                fwrite(outBuff[i].buffer, sizeof(enc_ch), length - 1, stdout);
            }
        }
        else {
            fwrite(outBuff[i].buffer, sizeof(enc_ch), length, stdout);
        }
    }
    length = outBuff[numChunks - 1].numElements;
    fwrite(outBuff[numChunks - 1].buffer, sizeof(enc_ch), length, stdout);
    
    // for (int i = 0; i < numChunks; i++) {
    //     if (outBuff[i].buffer == NULL) {
    //         printf("Null buffer entry occurred at index %d, with size %d and number of elements %d.\n", i, outBuff[i].size, outBuff[i].numElements);
    //     }
    // }

    // for (int i = 0; i < numFiles; i++) {
    //     map_close(fptrs[i], fsizes + i);
    // }
    // free(fptrs);
    // free(fsizes);
    // free(chunkBuff);
    // free(outBuff);

    // printf("Number of chunks = %d\n", numChunks);
    return 0;
}

char get_last_ch(int chunkID) {
    if (outBuff[chunkID].numElements > 1) {
        return outBuff[chunkID].buffer[(outBuff[chunkID].numElements - 1)].ch;
    }
    return get_first_ch(chunkID);
}

char get_first_ch(int chunkID) {
    return outBuff[chunkID].buffer[0].ch;
}

void set_first_num(int chunkID, int n) {
    outBuff[chunkID].buffer[0].num = n;
}

int get_first_num(int chunkID) {
    return outBuff[chunkID].buffer[0].num;
}

int get_last_num(int chunkID) {
    if (outBuff[chunkID].numElements > 1) {
        return outBuff[chunkID].buffer[(outBuff[chunkID].numElements - 1)].num;
    }
    return get_first_num(chunkID);
}

unproc_chunk *do_get() {
    // Get relevant chunk data from shared queue
    // unproc_chunk *tmp = &chunkBuff[useptr];
    unproc_chunk *tmp = (unproc_chunk *)Malloc(sizeof(unproc_chunk));
    tmp->id = chunkBuff[useptr].id;
    tmp->size = chunkBuff[useptr].size;
    tmp->loc = chunkBuff[useptr].loc;

    // Increment useptr with wrap around
    useptr = (useptr + 1) % QUEUE_SIZE;

    // Update the number of items in the queue
    numfull--;

    return tmp;
}

void do_fill(unproc_chunk *c) {
    // Fill relevant chunk data in shared queue
    chunkBuff[fillptr].id = c->id;
    chunkBuff[fillptr].size = c->size;
    chunkBuff[fillptr].loc = c->loc;

    // Increment fill ptr with wrap around
    fillptr = (fillptr + 1) % QUEUE_SIZE;

    // Update the number of items in the queue
    numfull++;
}

void *producer(void *arg) {
    int fileInd = 0;            // Index of current file in fptrs array
    int fileDone = 0;           // Flag, 1 if current file is done being chunked
    int chunkID = 0;            // Counter that gives chunks unique ID's
    int chunkOffset = 0;        // Chunk's offset from beginning of current file
    
    // Continue to produce chunks until last file is finished
    while (fileInd < numFiles || !fileDone) {

        // NICK - deals with empty file case
        if (fptrs[fileInd] == NULL) {
            fileInd++;
            continue;
        }
        
        // Reset done flag if file is done but not all files have been chunked
        if (fileDone) {
            fileDone = 0;
        }

        // Init empty chunk to be passed to queue
        unproc_chunk c = {-1, -1, NULL};   
        
        // Case 1: EOF isn't reached and a whole chunk can be taken
        if (chunkOffset + CHUNK_SIZE < fsizes[fileInd]) {
            c.size = CHUNK_SIZE;
            c.loc = fptrs[fileInd] + chunkOffset;
            chunkOffset += CHUNK_SIZE;
        }
        // Case 2: EOF is reached and EXACTLY a full chunk can be taken
        else if (chunkOffset + CHUNK_SIZE == fsizes[fileInd]) {
            c.size = CHUNK_SIZE;
            c.loc = fptrs[fileInd] + chunkOffset;
            chunkOffset = 0;
            fileDone = 1;
            fileInd++;
        }
        // Case 2: EOF is reached and less than full chunk can be taken
        else {
            c.size = fsizes[fileInd] - chunkOffset;
            c.loc = fptrs[fileInd] + chunkOffset;
            chunkOffset = 0;
            fileDone = 1;
            fileInd++;
        }
        c.id = chunkID;
        chunkID++;

        // printf("Produced chunk %d with size %d.\n", c.id, c.size);
        // fflush(stdout);

        // Aquire lock and fill shared queue
        Mutex_lock(&m);
        while (numfull == QUEUE_SIZE) {
            Cond_wait(&empty, &m);
        }
        do_fill(&c);
        Cond_signal(&fill);

        // If last chunk of last file, set global done flag to singal production's end
        if (fileInd == numFiles && fileDone) {
            // printf("Production complete!\n");
            // fflush(stdout);
            done = 1;
            numChunks = chunkID;
            Cond_broadcast(&fill);
        }
        Mutex_unlock(&m);
    }
    return NULL;
}

void *consumer(void *arg) {
    while (!done || numfull != 0) {
        Mutex_lock(&m);
        
        while (numfull == 0 && !done) {
            Cond_wait(&fill, &m);
        }

        if (numfull == 0 && done) {
            Mutex_unlock(&m);
            break;
        }

        unproc_chunk *tmp = do_get();
        Cond_signal(&empty);
        Mutex_unlock(&m);

        // printf("Chunk ID: %d, Chunk size: %d, Chunk location: %p\n", tmp->id, tmp->size, tmp->loc);
        // fflush(stdout);
        
        // Process chunk
        encode_chunk(tmp);
        // printf("\tSize of 2D buffer = %d\n", outBuff[tmp->id].size);
        // fflush(stdout);
    }
    // printf("Consumer exited.\n");
    // fflush(stdout);
    return NULL;
}

void encode_chunk(unproc_chunk *c) {
    proc_chunk *output = &outBuff[c->id];
    output->numElements = 0;
    output->size = INIT_OUT_BUFFSIZE;
    // printf("\tSize of 2D buffer = %d\n", output->size);
    // fflush(stdout);
    output->buffer = (enc_ch *)Malloc(INIT_OUT_BUFFSIZE * sizeof(enc_ch));

    char ch;
    char prev_ch = *(c->loc);
    int ch_count = 1;
    
    for (int i = 1; i < c->size; i++) {
        ch = *(c->loc + i);
        if (ch == prev_ch) {
            ch_count++;
        }
        else {
            add_enc_ch(output, ch_count, prev_ch);
            ch_count = 1;   //Reset char count
        }
        prev_ch = ch;
    }
    add_enc_ch(output, ch_count, prev_ch);
}

void add_enc_ch(proc_chunk *output, int ch_count, char prev_ch) {
    // If output buffer isn't large enough, resize
    output->numElements++;
    while (output->numElements > output->size) {
        output->size = output->size + (output->size >> 1) + INIT_OUT_BUFFSIZE;
    }
    output->buffer = (enc_ch *)Realloc(output->buffer, output->size * sizeof(enc_ch));

    // Add encoding to buffer
    (*(output->buffer + output->numElements - 1)).num = ch_count;
    (*(output->buffer + output->numElements - 1)).ch = prev_ch;
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
char *map_open(char *fname, int *size) {
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

    // NICK
    if (*size <= 0) {
        return NULL;
    }

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
void map_close(char *loc, int *size) {
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
    fflush(stdout);
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

void *Realloc(void *ptr, size_t size) {
    void *p = realloc(ptr, size);
    if (p == NULL) {
        exit_err("Realloc failed.");
    }
    return p;
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

void Cond_broadcast(pthread_cond_t *cond) {
    if (pthread_cond_broadcast(cond) != 0) {
        exit_err("Pthread broadcast failed.");
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