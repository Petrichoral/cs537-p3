#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

char *map_open(char *fname);
void exit_err(char *message);

int main(int argc, char *argv[]) {
    return 0;
}

char *map_open(char *fname) {
    struct stat fs; // Holds file info
    char *loc;      // Pointer to addr zero of mapped file
    int fd;         // File descriptor for passed file

    // Get the file descriptor for the specified filename
    if ((fd = open(fname, O_RDONLY)) == -1) {
        exit_err("Couldn't open file.");
    }

    // Get the file's size
    if (fstat(fd, &fs) == -1) {
        exit_err("Call to fstat failed.");
    }

    // Make sure fd points to a regular file (e.g. not a directory)
    if (!S_ISREG(fs.st_mode)) {
        exit_err("Passed file is irregular.")
    }

    // Attempt to map file
    loc = mmap(NULL, fs.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (loc == MAP_FAILED) {
        exit_err("Failed to map file.")
    }

    // Close the file descriptor
    if (close(fd) == -1) {
        exit_err("Failed to close file.")
    }

    // Return pointer to first address in the map
    return loc;
}

void exit_err(char *message) {
    printf("pzip: %s\n", message);
    exit(1);
}