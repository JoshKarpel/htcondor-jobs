import logging
import sys


import htcondor_jobs as jobs

l = logging.StreamHandler(sys.stdout)
l.setLevel(logging.DEBUG)
l.setFormatter(logging.Formatter("%(asctime)s ~ %(name)s ~ %(message)s"))
logger = logging.getLogger("htcondor_jobs")
logger.addHandler(l)
logger.setLevel(logging.DEBUG)

desc = jobs.SubmitDescription(executable="/bin/sleep", arguments="1", log="log")


def basic_diamond():
    yield jobs.submit(desc.copy(JobBatchName="a"))
    handles = [jobs.submit(desc.copy(JobBatchName=f"b{n}"), count=1) for n in range(5)]
    yield from handles
    yield jobs.submit(desc.copy(JobBatchName="c"))


def middle(n):
    print(f"SUBMITTING b{n}")
    yield jobs.submit(desc.copy(JobBatchName=f"b{n}"), count=1)
    print(f"SUBMITTING c{n}+")
    p = jobs.submit(desc.copy(JobBatchName=f"c{n}+"), count=1)
    print(f"SUBMITTING c{n}-")
    m = jobs.submit(desc.copy(JobBatchName=f"c{n}-"), count=1)
    yield from (p, m)
    print(f"postscript {n}")


def long_split_diamond():
    yield jobs.submit(desc.copy(JobBatchName="a"), count=1)

    yield (middle(n) for n in range(5))
    print(f"postscript after all middle done")

    yield jobs.submit(desc.copy(JobBatchName="d"), count=1)


def is_running_or_complete(handle):
    return all(
        s in (jobs.JobStatus.RUNNING, jobs.JobStatus.COMPLETED)
        for s in handle.state.values()
    )


def line_when_running():
    d = desc.copy(arguments="10")
    print("starting a")
    a = jobs.submit(d.copy(JobBatchName="a"), count=1)
    yield a, is_running_or_complete
    print("starting b")
    b = jobs.submit(d.copy(JobBatchName="b"), count=1)
    yield b, is_running_or_complete
    print("starting c")
    c = jobs.submit(d.copy(JobBatchName="c"), count=1)
    yield c, is_running_or_complete
    print("starting d")
    d = jobs.submit(d.copy(JobBatchName="d"), count=1)
    yield from (a, b, c, d)


def nested_top():
    yield jobs.submit(desc.copy(JobBatchName="top"), count=1)
    yield [nested_middle(n) for n in range(3)]


def nested_middle(n):
    yield jobs.submit(desc.copy(JobBatchName=f"middle-{n}"), count=1)
    yield [nested_bottom(n, m) for m in range(3)]


def nested_bottom(n, m):
    yield jobs.submit(desc.copy(JobBatchName=f"bottom-{n}-{m}"), count=1)


# jobs.execute(basic_diamond())
# jobs.execute(long_split_diamond())
# jobs.execute(line_when_running())
jobs.execute(nested_top())

# print(desc)
#
# handle = jobs.submit(desc, count=4)
#
# print(handle.clusterad)
#
# while not is_done(handle):
#     print(handle.state)
#     time.sleep(0.1)
#
# print(handle.state)


print("DONE")
