/* "module load datawarp" */
/* DW mount point on compute notes: $DW_JOB_STRIPED */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>

#include <datawarp.h>

/*
 * avail API calls (as per dw user guide from cray web site):
 * 
 * int dw_get_stripe_configuration(int fd,   // open fd of DW file/dir 
 *        int *stripe_size,                  // OUT 
 *        int *stripe_width,                 // OUT: max# of stripes avail 
 *        int *starting_index);              // OUT: 0..stripe_width 
 *  ret 0 or -errno
 *
 */

char *dw = NULL;

int main(int argc, char **argv) {
    int fd, rv;
    int size, width, start;
    printf("datawarp test program\n");
    dw = getenv("DW_JOB_STRIPED");
    if (!dw) {
        printf("datawarp not configured\n");
        exit(1);
    }

    printf("DW_JOB_STRIPED = %s\n", dw);
    fd = open(dw, O_RDONLY);
    if (fd < 0) {
        perror("open dw");
        exit(1);
    }
    printf("attempt on stdin: %d\n", 
         dw_get_stripe_configuration(0, &size, &width, &start));
    rv = dw_get_stripe_configuration(fd, &size, &width, &start);
    if (rv < 0) {
        fprintf(stderr, "dw_get_stripe_configuration failed %d\n", rv);
        exit(1);
    } else {
        printf("top-level stripe cfg: size=%d, width=%d, start=%d\n",
               size, width, start);
    }

    exit(0);
}
