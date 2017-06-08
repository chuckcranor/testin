/* "module load datawarp" */
/* DW mount point on compute notes: $DW_JOB_STRIPED */

#include <err.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
    if (!dw) errx(1, "datawarp not configured");

    rv = dw_get_stripe_configuration(0, &size, &width, &start);
    if (rv >= 0) 
        errx(1, "unexpected rv %d from dw_get_stripe_configuration", rv);
    printf("got %s on an invalid stripe cfg call\n", strerror(-rv));

    printf("DW_JOB_STRIPED = %s\n", dw);
    fd = open(dw, O_RDONLY);
    if (fd < 0) err(1, "can't open DW_JOB_STRIPED dir");

    rv = dw_get_stripe_configuration(fd, &size, &width, &start);
    if (rv < 0) 
        errx(1, "get stripe cfg err: %s", strerror(-rv));
    printf("top-level stripe cfg: size=%d, width=%d, start=%d\n",
           size, width, start);

    exit(0);
}
