
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>

int main() {
	/*
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
	*/
	void *p = mmap(NULL, 0x1L << 36, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE, -1, 0);
	if(p == MAP_FAILED) {
		printf("mmap() returns -1; reason = %s\n", strerror(errno));
	}
	

	while(1);

	return 0;
}
