#include <assert.h>
#include <inttypes.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include <pollqueue.h>

static uint64_t time_ms()
{
    struct timespec now;

    if (clock_gettime(CLOCK_MONOTONIC, &now))
        return 0;
    return (now.tv_nsec / 1000000) + (uint64_t)now.tv_sec * 1000;
}

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

//----------------------------------------------------------------------------

struct test2_env_ss
{
	short revents;
	uint64_t time;
	sem_t sem;
};

static void
test2_cb(void *v, short revents)
{
	struct test2_env_ss * const te = v;
	te->revents = revents;
	te->time = time_ms();
	sem_post(&te->sem);
}

static int
test2()
{
	struct pollqueue * pq;
	struct polltask * pt;
	struct test2_env_ss test2_env = {0};
	uint64_t now;
	uint64_t delta;
	int rv = 0;

	printf("Test2: Timers\n");

	sem_init(&test2_env.sem, 0, 0);

	pq = pollqueue_new();
	if (pq == NULL) {
		printf("Pollqueue create failed\n");
		return 1;
	}
	pt = polltask_new_timer(pq, test2_cb, &test2_env);
	if (pt == NULL) {
		printf("Polltask create failed\n");
		return 1;
	}
	now = time_ms();
	pollqueue_add_task(pt, 500);

	sem_wait(&test2_env.sem);
	delta = test2_env.time - now;
	printf("Delta = %" PRId64 "ms\n", delta);
	polltask_delete(&pt);
	if (pt != NULL) {
		printf("PT not NULL after delete\n");
		rv = 1;
	}

	if (delta < 490 || delta > 600) {
		printf("Delta out of range - should be 500 (allow 490-600)");
		rv = 1;
	}

	pollqueue_finish(&pq);
	if (pq != NULL) {
		printf("PQ not NULL after finish\n");
		rv = 1;
	}
	printf(rv ? "FAIL\n" : "OK\n");

	sem_destroy(&test2_env.sem);
	return rv;
}

//----------------------------------------------------------------------------

struct test3_env_ss
{
	short revents;
	uint64_t time;
	struct polltask * pt;
};

static void
test3_cb(void *v, short revents)
{
	struct test3_env_ss * const te = v;
	te->revents = revents;
	te->time = time_ms();
	polltask_delete(&te->pt);
}

static int
test3()
{
	struct pollqueue * pq;
	struct test3_env_ss test3_env = {0};
	uint64_t now;
	uint64_t delta;
	int rv = 0;

	printf("Test3: Timer + ref\n");

	pq = pollqueue_new();
	if (pq == NULL) {
		printf("Pollqueue create failed\n");
		return 1;
	}
	test3_env.pt = polltask_new_timer(pq, test3_cb, &test3_env);
	if (test3_env.pt == NULL) {
		printf("Polltask create failed\n");
		return 1;
	}
	now = time_ms();
	pollqueue_add_task(test3_env.pt, 500);

	pollqueue_finish(&pq);
	if (pq != NULL) {
		printf("PQ not NULL after finish\n");
		rv = 1;
	}

	delta = time_ms() - now;
	printf("Delta to finish = %" PRId64 "ms\n", delta);
	if (delta < 490 || delta > 600) {
		printf("Delta out of range - should be 500 (allow 490-600)");
		rv = 1;
	}

	delta = test3_env.time - now;
	printf("Delta to cb = %" PRId64 "ms\n", delta);
	if (delta < 490 || delta > 600) {
		printf("Delta out of range - should be 500 (allow 490-600)");
		rv = 1;
	}

	if (test3_env.revents != 0) {
		printf("Revents not zero\n");
		rv = 1;
	}

	printf(rv ? "FAIL\n" : "OK\n");
	return rv;
}

//----------------------------------------------------------------------------

static void
test4_cb(void *v)
{
	*(bool *)v = true;
}

static int
test4()
{
	struct pollqueue * pq;
	bool exit1 = false;
	bool exit2 = false;
	printf("Test4: On exit\n");

	pq = pollqueue_new();
	if (pq == NULL) {
		printf("Pollqueue create failed\n");
		return 1;
	}
	pollqueue_set_exit(pq, test4_cb, &exit1);
	pollqueue_set_exit(pq, test4_cb, &exit2);
	pollqueue_finish(&pq);
	if (pq != NULL) {
		printf("PQ not NULL after finish\n");
		return 1;
	}
	if (!(exit1 && exit2)) {
		printf("Failed to get both exit CBs (%d,%d)\n", exit1, exit2);
		return 1;
	}
	printf("OK\n");
	return 0;
}

//----------------------------------------------------------------------------

static void
test5_cb(void *v, short revents)
{
	(void)v;
	assert(revents == 0);
}

static int
test5()
{
	struct pollqueue * pq;
	uint64_t now;
	uint64_t delta;
	int t;
	int rv = 0;
	printf("Test5: finish_timeout\n");

	pq = pollqueue_new();
	if (pq == NULL) {
		printf("Pollqueue create failed\n");
		return 1;
	}
	now = time_ms();
	pollqueue_timer_once(pq, test5_cb, NULL, 1000);
	t = pollqueue_finish_timeout(&pq, 0);
	if (t != 1) {
		printf("First finish did not timeout (0)\n");
		rv = 1;
	}
	t = pollqueue_finish_timeout(&pq, 500);
	if (t != 1) {
		printf("Second finish did not timeout (500)\n");
		rv = 1;
	}
	delta = time_ms() - now;
	printf("Delta to finish 2 = %" PRId64 "ms\n", delta);
	if (delta < 490 || delta > 650) {
		printf("Delta out of range - should be 500 (allow 490-650)");
		rv = 1;
	}
	t = pollqueue_finish_timeout(&pq, 1000);
	if (t != 0) {
		printf("Third finish timedout (1000)\n");
		rv = 1;
	}
	delta = time_ms() - now;
	printf("Delta to finish 3 = %" PRId64 "ms\n", delta);
	if (delta < 990 || delta > 1100) {
		printf("Delta out of range - should be 1000 (allow 990-1100)");
		rv = 1;
	}

	if (pq != NULL) {
		printf("PQ not NULL after finish\n");
		return 1;
	}
	printf(rv ? "FAIL\n" : "OK\n");
	return 0;
}


//----------------------------------------------------------------------------

int
main(int argc, char *argv[])
{
	int fail_count = 0;
	(void)argc;
	(void)argv;

	fail_count += test1();
	fail_count += test2();
	fail_count += test3();
	fail_count += test4();
	fail_count += test5();

	if (fail_count)
		printf("Tests failed: %d\n", fail_count);
	else
		printf("All tests OK\n");
	return fail_count;
}

