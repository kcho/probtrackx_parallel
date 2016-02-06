import numpy
import pp
import os
import re
import sys
import random

def probtrackx2_parallel(args):
    tmpLocation = '/tmp/tractography_parallel'
    #serverList = {'M1':'147.47.228.230',
                  #'M2':'ccnc.snu.ac.kr',
                  #'M3':'147.47.238.248',
                  #'M7':'147.47.238.118'}
                  #'MT':'MT' }

    serverList = {'M2':'ccnc.snu.ac.kr',
                  'M3':'147.47.238.248',
                  'M7':'147.47.238.118'}

    marks = {'bedpostDir' : '-s',
             'nsamples':'-P',
             'maskFile' : '-m',
             'seedFile' : '-x',
             'outDir' : '--dir=',
             'waypoints' : '--waypoints=',
             'targetMasksFile' : '--targetmasks',
             'avoidFile' : '--avoid=',
             'stopFile' : '--stop=',
             'xfmFile' : '--xfm='}


    outDir = re.search('--dir=(\S+)', ' '.join(args)).group(1)
    print outDir

    fileDict = get_file_dict(args, marks)
    servers = serverList.values()
    data_dispatch(fileDict, tmpLocation, servers)
    nseed = 100
    rseed = get_rseed(nseed)
    cmds = makeCommand(args, fileDict, rseed, marks, tmpLocation)
    runCommands(servers, cmds)

    # Data back to here
    data_collect(rseed, servers, outDir, tmpLocation)

    # sum
    fdtList = [os.path.join(outDir,str(x),'fdt_paths.nii.gz') for x in rseed]
    waytotalList = [os.path.join(outDir,str(x),'waytotal') for x in rseed]

    totalValue = 0
    for waytotal in waytotalList:
        with open(waytotal, 'r') as f:
            value = int(f.read())
            totalValue += value
    with open(os.path.join(outDir, 'waytotal'), 'w') as f:
        f.write(totalValue)

    print 'fslmaths '+fdtList[0]+' -add '.join(fdtList[1:]) +' '+ os.path.join(outDir, 'total_fdt_paths.nii.gz')



def runCommands(servers, cmds):
    ppservers=tuple([x+':35000' for x in servers])
    job_server = pp.Server(ppservers=ppservers, secret="ccnc")
    #job_server = pp.Server(ppservers=ppservers, secret="nopassword")
    #job_server = pp.Server(ppservers=ppservers, secret="mysecret")
    #job_server = pp.Server(ncpus, ppservers=ppservers, secret="ccncserver")
    #ncpus = 20
    #run_pp_server(servers)
    #print "Starting pp with", job_server.get_ncpus(), "workers"
    jobs = [(cmd,
             job_server.submit(run,
                               (cmd,),
                               () ,
                               ("os",))) for cmd in cmds]

    for command, job in jobs:
        print command, "is completed", job()

def data_collect(rseed, servers, outDir, tmpLocation):
    outDirs = [tmpLocation+'/'+str(x) for x in rseed]
    for server in servers:
        for outDir in outDirs:
            scpCommand = 'scp -r {server}:{outDir} \
                                 {outDir}'.format(
                    server = server,
                    outDir = outDir)
            #print scpCommand
            os.popen(scpCommand).read()

    print '========================='
    print 'Data collection completed'
    print '========================='



def makeCommand(args, fileDict, rseed, marks, tmpLocation):
    print fileDict
    for markName, fileLocation in fileDict.iteritems():
        if markName == 'nsamples':
            index = args.index(marks[markName]) + 1
            args[index] = 50
        elif markName in ['outDir', 'waypoints', 'avoidFile', 'stopFile', 'xfmFile' ]:
            index = args.index(''.join([x for x in args if str(x).startswith(marks[markName])]))
            fileBasename = os.path.basename(fileLocation)
            args[index] = re.sub(marks[markName]+'\S+',
                    marks[markName]+tmpLocation + '/' + fileBasename,
                    args[index])
        #elif markName == 'bedpostDir':
            #index = args.index(marks[markName]) + 1
            #fileBasename = os.path.basename(fileLocation)
            #args[index] = tmpLocation + '/' + fileBasename
        elif fileLocation != '':
            index = args.index(marks[markName]) + 1
            fileBasename = os.path.basename(fileLocation)
            args[index] = tmpLocation + '/' + fileBasename
        else:
            pass

    cmds = []
    args = [str(arg) for arg in args]
    for num in rseed:
        #outDirIndex = args.index('-d') + 1
        newArgs = re.sub('dir={0}'.format(tmpLocation),
                'dir={0}/{1}'.format(tmpLocation, num),
                ' '.join(args))
        newCommand = '/usr/local/fsl/bin/probtrackx2 '+ newArgs + ' --rseed={0}'.format(num)
        cmds.append(newCommand)
    return cmds

def get_rseed(nseed):
    nseed = 100
    randNumbers = []
    for num in range(nseed):
        randNumbers.append(random.randint(1235, 9999))
    return randNumbers

def get_file_dict(args, marks):
    fileDict = {}
    for markname, mark in marks.iteritems():
        try:
            if '=' in mark:
                fileLocation = re.search(mark+'(\S+)', ' '.join(args)).group(1)
                fileDict[markname] = fileLocation
            else:
                fileLocation = args[args.index(mark) + 1]
                fileDict[markname] = fileLocation
        except:
            fileDict[markname] = ''

    print fileDict
    return fileDict

def run_pp_server(servers):
    for server in servers:
        command = "ssh {server} 'ppserver.py' &"
        os.popen(scpCommand).read()

def data_dispatch(fileDict, tmpLocation, servers):
    sourceFiles = [x for x in fileDict.values() if x != '']
    for server in servers:
        mkdirCommand = "ssh {server} 'rm -rf {tmpLocation};mkdir {tmpLocation}'".format(
                    server=server,
                    tmpLocation = tmpLocation)
        print mkdirCommand
        os.popen(mkdirCommand).read()
        for sourceFile in sourceFiles:
            if 'merged' in sourceFile:
                scpCommand = 'scp -r {sourceFile}* \
                                     {server}:{tmpLocation}'.format(
                        sourceFile = sourceFile,
                        server = server,
                        tmpLocation = tmpLocation)
            else:
                scpCommand = 'scp -r {sourceFile} \
                                     {server}:{tmpLocation}'.format(
                        sourceFile = sourceFile,
                        server = server,
                        tmpLocation = tmpLocation)

            print scpCommand
            os.popen(scpCommand).read()

    print '======================='
    print 'Data dispatch completed'
    print '======================='


def run(job):
    os.popen(job).read()






if __name__ == '__main__':
    probtrackx2_parallel(sys.argv[1:])



##!/Users/admin/anaconda/bin/python

#import math, sys, time
#import pp
#import os


#print """Usage: python sum_primes.py [ncpus]
    #[ncpus] - the number of workers to run in parallel,
    #if omitted it will be set to the number of processors in the system
#"""


## tuple of all parallel python servers to connect with
##ppservers = ("*",)
##ppservers = ("10.0.0.1",)
##if len(sys.argv) > 1:
##ncpus = int(sys.argv[1])
## Creates jobserver with ncpus workers
##else:
## Creates jobserver with automatically detected number of workers
##job_server = pp.Server(ppservers=ppservers, secret="ccncserver")


## Submit a job of calulating sum_primes(100) for execution.
## sum_primes - the function
## (100,) - tuple with arguments for sum_primes
## (isprime,) - tuple with functions on which function sum_primes depends
## ("math",) - tuple with module names which must be imported before sum_primes execution
## Execution starts as soon as one of the workers will become available
##job1 = job_server.submit(bet, (100,), (isprime,), ("math",))

## Retrieves the result calculated by job1
## The value of job1() is the same as sum_primes(100)
## If the job has not been finished yet, execution will wait here until result is available
##result = job1()

##print "Sum of primes below 100 is", result

#start_time = time.time()

## The following submits 8 jobs and then retrieves the results
#commandFile = '/Volumes/CCNC_3T_2/kcho/script.sh'

#with open(commandFile,'r') as f:
    #commandsToRun = f.readlines()

#jobs = [(command,
         #job_server.submit(jobDispatch,
                           #(command,),
                           #() ,
                           #("os","sys","re","argparse","textwrap",))) for command in commandsToRun]

#for command, job in jobs:
    #print command, "is completed", job()

#print "Time elapsed: ", time.time() - start_time, "s"
#job_server.print_stats()
