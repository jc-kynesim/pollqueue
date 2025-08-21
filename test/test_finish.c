#include <stdio.h>
#include <pollqueue.h>

static int
test1()
{
	struct pollqueue * pq;
	printf("Test1: Create & finish\n");
	pq = pollqueue_new();
	if (pq == NULL) {
		printf("Pollqueue create failed\n");
		return 1;
	}
	pollqueue_finish(&pq);
	if (pq != NULL) {
		printf("PQ not NULL after finish\n");
		return 1;
	}
	printf("OK\n");
	return 0;
}

int
main(int argc, char *argv[])
{
	int fail_count = 0;

	fail_count += test1();

	return fail_count;
}

