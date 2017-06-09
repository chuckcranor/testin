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
 * CFG:
 * int dw_get_stripe_configuration(int fd,   // open fd of DW file/dir 
 *        int *stripe_size,                  // OUT 
 *        int *stripe_width,                 // OUT: max# of stripes avail 
 *        int *starting_index);              // OUT: 0..stripe_width 
 *  ret 0 or -errno
 * int dw_set_stripe_configuration(int fd, int stripe_size, int stripe_width);
 *  - behavior after file has data in it is undefined (set before data)
 *  - it is persistent
 *  - file inherit directory config
 *  - subdirs do not inherit stripe config of parent
 *  - stripe size should be multiple of instance default (to avoid space waste)
 * int dw_get_mds_path(const char *dw_root, uint64_t key);
 *    (key: app provided val used ot determine mds)
 * ret: string w/path prefix for given input key (malloc'd, caller frees) 
 *
 * int dw_set_stage_concurrency(const char *dw_instance_path,
 *                      unsigned int concurrent_stages);
 *   // max number of concurrent stage ops we allow
 *
 * ASYNC STAGING OP QUERY:
 * int dw_query_directory_stage(const char *dw_directory_path, // IN
 *                           int *complete,  int *pending,     // OUT
 *                           int *deferred, int *failed);      // OUT
 * int dw_query_file_stage(const char *dw_file_path,           // IN
 *                           int *complete, int *pending,      // OUT
 *                           int *deferred, int *failed);      // OUT
 * int dw_query_list_stage(const char **dw_list_path,          // IN
 *                           int *complete, int *pending,      // OUT
 *                           int *deferred, int *failed);      // OUT
 * ret 0 (stats of stage op filled out), or -errno
 *
 * INIT ASYNC STAGE OP:  (only one active per file/dir)
 * IN: 
 * int dw_stage_directory_in(const char *dw_dir, const char *pfs_dir);
 * int dw_stage_file_in(const char *dw_file_path, const char *pfs_file_path);
 * int int dw_stage_list_in(const char *dw_directory, const char **pfs_list);
 * OUT:  
 * int dw_stage_directory_out(const char *dw_dir, const char *pfs_dir,
 *                         enum dw_stage_type stage_type)
 * int dw_stage_file_out(const char *dw_file_path, const char *pfs_file_path,
 *                    enum dw_stage_type stage_type); 
 * int dw_stage_list_out(const char **dw_list, const char *pfs_directory,
 *                    enum dw_stage_type stage_type);
 * ret 0 or -errno
 * OUT types: STAGE_IMMEDIATE, STAGE_AT_JOB_END, REVOKE_STAGE_AT_JOB_END
 * DW_ACTIVATE_DEFERRED_STAGE (turns "at job end" into "immediate")
 * "REVOKE_STAGE_AT_JOB_END" cancels a previous "STAGE_AT_JOB_END"
 *
 * list is a list of regular files
 * directory stage is recursive, XXX: top level made for us?
 * 
 * TERMINATE STAGE OP:
 * int dw_terminate_directory_stage(const char *dw_directory_path);
 * int dw_terminate_file_stage(const char *dw_file_path);
 * int dw_terminate_list_stage(const char **dw_list);
 * ret 0, -EINVAL (no op pending), -errno
 * XXX: list.. does it have to be same array that started op?  e.g.
 * same pointer?  or if not, same list of files?
 *
 * WAIT FOR STAGE OP TO COMPLETE:
 * int dw_wait_directory_stage(const char *dw_directory_path);
 * int dw_wait_file_stage(const char *dw_file_path);
 * int dw_wait_list_stage(const char **dw_list);
 * ret 0, -EINVAL (no stage op pending?), -errno
 * XXX: list.. does it have to be same array that started op?  e.g.
 * same pointer?  or if not, same list of files?
 * can't interrupt with signal
 *
 * FAILED STAGE ID:   (dw_failed_stage_t is a struct)
 * int dw_open_failed_stage(const char *dw_instance_path, // IN
 *                      dw_failed_stage_t **handle);      // OUT
 * int dw_read_failed_stage(dw_failed_stage_t *handle, char *path, 
 *                      int path_size, int *ret_errno);
 * int dw_close_failed_stage(dw_failed_stage_t *handle);
 * ret 0 or -errno
 * no seek, close and reopen to reset
 * failure remains until: file unlinked, stage terminated, stage restart
 * note that failure list can change while it is being walked
 */

int cat(char *file) {
    char buf[512];
    ssize_t rv;
    fd = open(file, O_RDONLY);
    if (fd < 0) return(-1);
    printf("content of %s:\n", file);
    while ((rv = read(fd, buf, sizeof(buf))) > 0) {
      if (rv < 0) warn("read %s failed", file);
      write(fileno(stdout), buf, rv);   /* ignore errors */
    }
    printf("\nEND OF FILE (%s)\n", file);
    return(0);
}

char *dw = NULL;

int main(int argc, char **argv) {
    int fd, rv;
    int size, width, start;
    ssize_t sret;
    char buf[BUFSIZ], buf2[BUFSIZ], buf3[BUFSIZ];
    setlinebuf(stdout);
    printf("datawarp test program\n");
    dw = getenv("DW_JOB_STRIPED");
    if (!dw) errx(1, "datawarp not configured");
    if (dw[strlen(dw)-1] != '/') 
        errx(1, "DW_JOB_STRIPED does not end in a slash");

    printf("config test\n");
    rv = dw_get_stripe_configuration(0, &size, &width, &start);
    if (rv >= 0) 
        errx(1, "unexpected rv %d from dw_get_stripe_configuration", rv);
    printf("got '%s' on an invalid stripe cfg call\n", strerror(-rv));

    printf("DW_JOB_STRIPED = %s\n", dw);
    fd = open(dw, O_RDONLY);
    if (fd < 0) err(1, "can't open DW_JOB_STRIPED dir");

    rv = dw_get_stripe_configuration(fd, &size, &width, &start);
    if (rv < 0) 
        errx(1, "get stripe cfg err: %s", strerror(-rv));
    printf("top-level stripe cfg: size=%d, width=%d, start=%d\n",
           size, width, start);
    close(fd);

    printf("staging test\n");
    snprintf(buf, sizeof(buf), "%stestin", dw);
    snprintf(buf2, sizeof(buf2), "%stestin/file", dw);
    snprintf(buf3, sizeof(buf2), "%stestin/bogus", dw);
    if (mkdir(buf, 0777) < 0) err(1, "mkdir %s", buf);
    printf("mkdir %s success\n", buf);
    if ((fd = open(buf2, O_WRONLY|O_CREAT, 0666)) < 0) err(1, "open %s", buf2);
    printf("file create %s success\n", buf2);

#define MSG "Hello world, part1!\n"
    if ((sret = write(fd, MSG, sizeof(MSG)-1)) != sizeof(MSG)-1)
        errx(1, "write bad ret %d", sret);

    printf("attempting stageout for non-dw bogus filename\n");
    rv = dw_stage_file_out("/tmp/bogus", "/lustre/ttscratch1/ccranor/st/ok",
             DW_STAGE_IMMEDIATE);
    printf("non-dw bogus result: %s\n", strerror(-rv));

    printf("attempting stageout for dw bogus filename\n");
    rv = dw_stage_file_out(buf3, "/lustre/ttscratch1/ccranor/st/ok",
             DW_STAGE_IMMEDIATE);
    printf("dw bogus result: %s\n", strerror(-rv));

    printf("attempting stageout for good dw filename (that is open)\n");
    rv = dw_stage_file_out(buf2, "/lustre/ttscratch1/ccranor/st/ok",
             DW_STAGE_IMMEDIATE);
    printf("real stage result: %s\n", strerror(-rv));
    if (rv != 0) errx(1, "stage didn't start!");

    printf("waiting for stage to complete\n");
    sleep(5);   /* let it finish first to see if this still works */
    rv = dw_wait_file_stage(buf2);
    printf("state complete wait ret: %s\n", strerror(-rv));

    printf("looking at files\n");
    cat(buf2);
    cat("/lustre/ttscratch1/ccranor/st/ok");

    printf("writing more to DW\n");
#define MSG "looks like I just wrote more!\n"
    if ((sret = write(fd, MSG, sizeof(MSG)-1)) != sizeof(MSG)-1)
        errx(1, "write bad ret %d", sret);
    
    printf("relooking at files\n");
    cat(buf2);
    cat("/lustre/ttscratch1/ccranor/st/ok");

    printf("attempting second stageout for good dw filename (that is open)\n");
    rv = dw_stage_file_out(buf2, "/lustre/ttscratch1/ccranor/st/ok",
             DW_STAGE_IMMEDIATE);
    printf("real stage result: %s\n", strerror(-rv));
    if (rv != 0) errx(1, "stage didn't start!");

    printf("waiting for stage to complete\n");
    sleep(5);   /* let it finish first to see if this still works */
    rv = dw_wait_file_stage(buf2);
    printf("state complete wait ret: %s\n", strerror(-rv));

    printf("looking at files after second stage\n");
    cat(buf2);
    cat("/lustre/ttscratch1/ccranor/st/ok");

    exit(0);
}
