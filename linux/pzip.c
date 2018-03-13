#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/sysinfo.h>

char *map_open(char *fname, off_t *size);
void map_close(char *loc, off_t *size);
void exit_err(char *message);

int main(int argc, char *argv[]) {
    // If no arguments are given, print usage
    if (argc == 1) {
        printf("pzip: file1 [file2 ...]\n");
        exit(1);
    }

    // Map files into memory
    int numFiles = argc - 1;
    char *fptrs[numFiles];
    off_t fsizes[numFiles];
    for (int i = 0; i < numFiles; i++) {
        fptrs[i] = map_open(argv[i + 1], &fsizes[i]);
    }
    
    for (int j = 0; j < numFiles; j++) {
        printf("File location: %p, File size: %ld\n", fptrs[j], fsizes[j]);
    }

    //Find out how many cores the CPU has
    int cores = get_nprocs_conf();
    printf("\nNumber of availible cores: %d\n", cores);

    return 0;
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