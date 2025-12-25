#include <stdio.h>
#include <sys/mount.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
//  sudo mount -t f2fs  /dev/loop23 /home/micron/f2fs_mount -nobarrier -sungjin

// a.out nobarrier,sungjin=2
/*
0 NORUNTIME
1 EZRESET
2 FAR
*/
char buf[128];


// sudo mount -o discard  -o discard_unit=segment /dev/nvme0n1 /home/femu/f2fs_fdp_mount/


int main(int argc, char** argv) {
    if(argc!=2){
        printf("%s <nlogs>\n",argv[0]);
        return 0;
    }
   const char *source = "/dev/nvme0n1";   // Source device (e.g., /dev/sda1)
    const char *target = "/home/femu/f2fs_fdp_mount"; // Target directory to mount on
    const char *filesystemtype = "f2fs"; // Filesystem type (e.g., ext4, vfat, etc.)
    unsigned long mountflags = 0;       // Mount options (e.g., read-only, noexec, etc.)

    // if(atoi(argv[1])==1){
//        sprintf(buf,"fdp_log_n=%d,discard,nobarrier,discard_unit=segment",atoi(argv[1]));
 sprintf(buf,"fdp_log_n=%d,nodiscard,nobarrier",atoi(argv[1]));  
  // }else{

    // }

    // const char *data = "nobarrier,sungjin=2"; // Filesystem-specific data/options

    // sprintf(buf,"nobarrier,sungjin=%d",atoi(argv[1]));
    
    // Create the target directory if it doesn't exist
    // if (mkdir(target, 0755) == -1 && errno != EEXIST) {
    //     perror("mkdir");
    //     return 1;
    // }

    // Perform the mount operation
    if (mount(source, target, filesystemtype, mountflags, buf) == -1) {
        printf("Failed mounted %s on %s with options: %s\n", source, target, buf);
        perror("mount");
        return 1;
    }

    printf("Successfully mounted %s on %s with options: %s\n", source, target, buf);
    return 0;
}
