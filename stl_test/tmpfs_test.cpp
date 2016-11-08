
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

int main() {
	int fd = open("/tmp/bwtree.tmp", O_RDWR | O_TRUNC | O_CREAT);
	if(fd == -1) {
		printf("open() returned -1\n");
		return -1;
	}

	int ret = ftruncate(fd, (0x1L << 38));
	if(ret == -1) {
		printf("ftruncate() returns -1; reason = %s\n", strerror(errno));
	}

	struct stat s;
	fstat(fd, &s);

	printf("file size = %lu\n", s.st_size);

	return 0;
}
