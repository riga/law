# coding: utf-8


import time

import law


def test_htcondor():
    # load the htcondor contrib package, making all exported objects available under law.htcondor
    law.contrib.load("htcondor")

    # create a job manager and job file factory
    manager = law.htcondor.HTCondorJobManager()
    factory = law.htcondor.HTCondorJobFileFactory()

    # htcondor expects an executable file, so create one that just does a simple directory listing
    executable = law.LocalFileTarget(is_tmp=True)
    executable.touch("ls")
    executable.chmod(0o0770)  # rwxrwx---, might not be needed though

    # create a job file
    job_file = factory.create(executable=executable.path)
    print("create job file {}".format(job_file))
    print("content:\n{}\n".format(open(job_file, "r").read()))

    """
    # submit the job
    # the return value will always be a list of job ids as htcondor supports cluster submission,
    # i.e., a single job file can result in multiple jobs
    job_id = manager.submit(job_file)[0]
    print("submitted job, id is {}\n".format(job_id))

    # start querying the status
    print("start status queries")
    for _ in range(1000):
        time.sleep(5)
        response = manager.query(job_id)
        print(response)

        if response["status"] not in ("pending", "running"):
            print("stop querying")
            break
    """

def test_slurm():
    law.contrib.load("slurm")

    manager = law.slurm.SlurmJobManager()
    factory = law.slurm.SlurmJobFileFactory()

    executable = law.LocalFileTarget(is_tmp=True)
    executable.touch("echo \"Hello World!\"")
    executable.chmod(0o0770)  # rwxrwx---, might not be needed though

    job_file1 = factory.create(file_name='job1.sh',
                               #executable=executable.path,
                               executable=executable.open(mode='r').readlines(),
                               ntasks=1,
                               custom_content="\n")
    job_file2 = factory.create(file_name='job2.sh', 
                               #executable=executable.path,
                               executable=executable.open(mode='r').readlines(),
                               ntasks=1,
                               custom_content="\n")
    print("create job files {} and {}".format(job_file1, job_file2))
    print("content:\n{}\n\n{}\n".format(open(job_file1, "r").read(),
                                        open(job_file2, "r").read()))

    job_id1 = manager.submit(job_file1)[0]
    job_id2 = manager.submit(job_file2)[0]
    print("submitted job, ids are {} and {}\n".format(job_id1, job_id2))

    print("start status queries")
    for _ in range(1000):
        time.sleep(.5)
        response = manager.query((job_id1,job_id2), user='balves')
        print('RESPONSE: ', response)

        if (( response[job_id1]["status"] not in ("pending", "running") ) or    
            ( response[job_id2]["status"] not in ("pending", "running") )):
            print("stop querying")
            break

if __name__ == "__main__":
    #test_htcondor()
    test_slurm()
