import numpy
import pp
import os
import re
import sys
import random

def probtrackx2_parallel(args):
    tmpLocation = '/tmp/tractography_parallel'
    serverList = {'M1':'147.47.228.230',
                  'M2':'147.28.228.253',
                  'M3':'147.47.238.248',
                  'M7':'147.47.238.118',
                  'MT':'147.46.196.62' }

    marks = {'bedpostDir' : '-s',
             'maskFile' : '-m',
             'seedFile' : '-x',
             'outDir' : '-d',
             'waypoints' : '--waypoints',
             'targetMasksFile' : '--targetmasks',
             'avoidFile' : '--avoid',
             'stopFile' : '--stop',
             'xfmFile' : '--xfm'}

    outDir = args[args.index('-d')+1]
    fileDict = get_file_dict(args, marks)
    #run_pp_server(servers)
    #data_dispatch(fileDict, tmpLocation)
    nseed = 100
    rseed = get_rseed(nseed)

    cmds = makeCommand(args, fileDict, rseed, marks)
    print cmds

    # run using pp

    # Data back to here
    #data_collect(rseed, servers, outDir)

    # sum
    fdtList = [os.path.join(outDir,str(x),'fdt_paths.nii.gz') for x in rseed]
    waytotalList = [os.path.join(outDir,str(x),'waytotal') for x in rseed]

    #totalValue = 0
    #for waytotal in waytotalList:
        #with open(waytotal, 'r') as f:
            #value = int(f.read())
            #totalValue += value
    #with open(os.path.join(outDir, 'waytotal'), 'w') as f:
        #f.write(totalValue)

    print 'fslmaths '+fdtList[0]+' -add '.join(fdtList[1:]) +' '+ os.path.join(outDir, 'total_fdt_paths.nii.gz')


def data_collect(rseed, servers, outDir):
    outDirs = ['/tmp/tractography_parallel/'+x for x in rseed]
    for server in servers:
        for directory in outDirs:
            scpCommand = 'scp -r {server}:{directory} \
                                 {outDir}'.format(
                    server = server,
                    outDir = outDir)

    print '========================='
    print 'Data collection completed'
    print '========================='



def makeCommand(args, fileDict, rseed, marks):
    for markName, fileLocation in fileDict.iteritems():
        if markName == 'bedpostDir':
            args[args.index(marks['bedpostDir']) + 1] = '/tmp/tractography_parallel/merged'
        elif markName == 'outDir':
            args[args.index(marks['outDir']) + 1] = '/tmp/tractography_parallel/merged'
        elif fileLocation != '':
            index = args.index(marks[markName]) + 1
            fileBasename = os.path.basename(fileLocation)
            args[index] = '/tmp/tractography_parallel/' + fileBasename
        else:
            pass

    cmds = []
    for num in rseed:
        outDirIndex = args.index('-d') + 1
        args[outDirIndex] = '/tmp/tractography_parallel/{0}'.format(num)
        newCommand = 'probtrackx2 '+ ' '.join(args) + ' --rseed={0}'.format(num)
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
            fileLocation = args[args.index(mark) + 1]
            fileDict[markname] = fileLocation
        except:
            fileDict[markname] = ''

    return fileDict

def run_pp_server(servers):
    for server in servers:
        command = "ssh {server} 'ppserver.py' &"
        os.popen(scpCommand).read()

def data_dispatch(fileDict, targetDir):
    sourceFiles = [x for x in fileDict.values() if x != '']
    for server in servers:
        for sourceFile in sourceFiles:
            if 'merged' in sourceFile:
                scpCommand = 'scp -r {sourceFile}* \
                                     {server}:{targetDir}'.format(
                        sourceFile = sourceFile,
                        server = server,
                        targetDir = targetDir)
            else:
                scpCommand = 'scp -r {sourceFile} \
                                     {server}:{targetDir}'.format(
                        sourceFile = sourceFile,
                        server = server,
                        targetDir = targetDir)
            os.popen(scpCommand).read()

    print '======================='
    print 'Data dispatch completed'
    print '======================='

    
    





if __name__ == '__main__':
    probtrackx2_parallel(sys.argv[1:])
