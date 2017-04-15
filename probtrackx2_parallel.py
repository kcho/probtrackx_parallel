from __future__ import division
import numpy as np
import nibabel as nb
from os.path import isdir, join, basename, dirname
from multiprocessing import Pool
import textwrap
import os
import re
import sys
import random
import argparse
import glob


def probtrackx2_parallel(args):

    print('Running probabilistic tractography in parallel')

    # load seed file
    f = nb.load(args.seedmask)
    data = f.get_data()
    coordinates = np.argwhere(data==1)

    if isdir(args.outdir):
        pass
    else:
        os.mkdir(args.outdir)

    print('Number of voxels in the given seed : {0}'.format(len(coordinates)))


    # Make voxel-wise input parameters in a list
    inputList = []
    for num, coordinate in enumerate(coordinates):
        inputList.append((coordinate, join(args.outdir, str(num)),
                            args.bedpostxdir, args.fsmask, args.regmat))

    # Parallization
    pool = Pool(10)
    for i,_ in enumerate(pool.imap_unordered(voxel_tractography, inputList), 1):
        sys.stderr.write('\rdone {0:%}'. format(i/len(inputList)))



def voxel_tractography(args):
    '''
    Article about random seed : https://www.jiscmail.ac.uk/cgi-bin/webadmin?A2=fsl;daca6637.1603
    '''
    coordinate, outdir, bedpostxdir, fsmask, regmat = args

    # make outdir
    if isdir(outdir):
        pass
    else:
        os.mkdir(outdir)

    # write coordinate file
    np.savetxt(join(outdir, 'fdt_coordinates.txt'), 
               coordinate, 
               delimiter=' ', 
               fmt='%d', 
               newline=' ')

    command = '/usr/share/fsl/5.0/bin/probtrackx2 \
                    --simple \
                    --seedref={fsmask} \
                    -x {fdt_coordinates} \
                    -l \
                    --onewaycondition \
                    -c 0.2 \
                    -S 2000 \
                    --steplength=0.5 \
                    -P 5000 \
                    --fibthresh=0.01 \
                    --distthresh=0.0 \
                    --sampvox=0.0 \
                    --xfm={regmat} \
                    --forcedir \
                    --opd \
                    -s {bedpostxdir}/merged \
                    -m {bedpostxdir}/nodif_brain_mask \
                    --dir={outdir}'.format(fsmask = fsmask, fdt_coordinates = join(outdir, 'fdt_coordinates.txt'),
                                           bedpostxdir = bedpostxdir, regmat = regmat, 
                                           outdir = outdir)
    command = re.sub('\s+', ' ', command)

    os.popen(command).read()
    #for coordinate in coordinates:
        

    #tmpLocation = '/tmp/tractography_parallel'
    ##serverList = {'M1':'147.47.228.230',
                  ##'M2':'ccnc.snu.ac.kr',
                  ##'M3':'147.47.238.248',
                  ##'M7':'147.47.238.118'}
                  ##'MT':'MT' }

    ##serverList = {'M2':'ccnc.snu.ac.kr',
                  ##'M1':'brainimage.snu.ac.kr',
                  ##'M3':'147.47.238.248',
                  ##'M7':'147.47.238.118'}

    #serverList = {'M1':'brainimage.snu.ac.kr'}

    #marks = {'bedpostDir' : '-s',
             #'nsamples':'-P',
             #'maskFile' : '-m',
             #'seedFile' : '-x',
             #'outDir' : '--dir=',
             #'waypoints' : '--waypoints=',
             #'targetMasksFile' : '--targetmasks',
             #'avoidFile' : '--avoid=',
             #'stopFile' : '--stop=',
             #'xfmFile' : '--xfm='}


    #outDir = re.search('--dir=(\S+)', ' '.join(args)).group(1)
    #print outDir

    #fileDict = get_file_dict(args, marks)
    #servers = serverList.values()
    #data_dispatch(fileDict, tmpLocation, servers)
    #nseed = 100
    #rseed = get_rseed(nseed)
    #cmds = makeCommand(args, fileDict, rseed, marks, tmpLocation)
    #runCommands(servers, cmds)

    ## Data back to here
    #data_collect(rseed, servers, outDir, tmpLocation)

    ## sum
    #fdtList = glob.glob(tmpLocation+'/[1234567890]*/*/fdt_paths.nii.gz')
    ##[os.path.join(outDir,str(x),'fdt_paths.nii.gz') for x in rseed]
    #waytotalList = glob.glob(tmpLocation+'/[1234567890]*/*/waytotal')
    ##for root, dirs, files in os.walk(tmpLocation):
        ##if 'waytotal' in files:
            ##waytotalList.append(os.path.join(root,'waytotal'))
    ##waytotalList = [os.path.join(outDir,str(x),'waytotal') for x in rseed]

    #totalValue = 0
    #for waytotal in waytotalList:
        #print waytotal
        #with open(waytotal, 'r') as f:
            #value_init = f.read()
            #print value_init
            #value = int(value_init)
            #totalValue += value
    #print totalValue
    #with open(os.path.join(tmpLocation, 'waytotal'), 'w') as f:
        #f.write(str(totalValue))

    #command = 'fslmaths '+fdtList[0]+' -add '+ ' -add '.join(fdtList[1:]) +' '+ os.path.join(tmpLocation, 'total_fdt_paths.nii.gz')
    #print command
    #print os.popen(command).read()



#def runCommands(servers, cmds):
    #ppservers=tuple([x+':35000' for x in servers])
    #job_server = pp.Server(12, ppservers=ppservers, secret="ccnc")
    ##job_server = pp.Server(ppservers=ppservers, secret="nopassword")
    ##job_server = pp.Server(ppservers=ppservers, secret="mysecret")
    ##job_server = pp.Server(ncpus, ppservers=ppservers, secret="ccncserver")
    ##ncpus = 50
    ##run_pp_server(servers)
    ##print "Starting pp with", job_server.get_ncpus(), "workers"
    #jobs = [(cmd,
             #job_server.submit(run,
                               #(cmd,),
                               #() ,
                               #("os",))) for cmd in cmds]

    
    #for num, (command, job) in enumerate(jobs):
        #print num,'\t', command, "is completed", job()

#def data_collect(rseed, servers, outDir, tmpLocation):
    #outDirs = [tmpLocation+'/'+str(x) for x in rseed]
    #for server in servers:
        #for outDir in outDirs:
            #scpCommand = 'scp -r {server}:{outDir} \
                                 #{outDir}'.format(
                    #server = server,
                    #outDir = outDir)
            #print scpCommand
            ##os.popen(scpCommand).read()

    #print '========================='
    #print 'Data collection completed'
    #print '========================='



#def makeCommand(args, fileDict, rseed, marks, tmpLocation):
    #print fileDict
    #for markName, fileLocation in fileDict.iteritems():
        #if markName == 'nsamples':
            #index = args.index(marks[markName]) + 1
            #args[index] = 50
        #elif markName in ['outDir', 'waypoints', 'avoidFile', 'stopFile', 'xfmFile' ]:
            #index = args.index(''.join([x for x in args if str(x).startswith(marks[markName])]))
            #fileBasename = os.path.basename(fileLocation)
            #args[index] = re.sub(marks[markName]+'\S+',
                    #marks[markName]+tmpLocation + '/' + fileBasename,
                    #args[index])
        ##elif markName == 'bedpostDir':
            ##index = args.index(marks[markName]) + 1
            ##fileBasename = os.path.basename(fileLocation)
            ##args[index] = tmpLocation + '/' + fileBasename
        #elif fileLocation != '':
            #index = args.index(marks[markName]) + 1
            #fileBasename = os.path.basename(fileLocation)
            #args[index] = tmpLocation + '/' + fileBasename
        #else:
            #pass

    #cmds = []
    #args = [str(arg) for arg in args]
    #for num in rseed:
        ##outDirIndex = args.index('-d') + 1
        #newArgs = re.sub('dir={0}'.format(tmpLocation),
                #'dir={0}/{1}'.format(tmpLocation, num),
                #' '.join(args))
        #newCommand = '/usr/local/fsl/bin/probtrackx2 '+ newArgs + ' --rseed={0}'.format(num)
        #cmds.append(newCommand)
    #return cmds

#def get_rseed(nseed):
    #nseed = 100
    #randNumbers = []
    #for num in range(nseed):
        #randNumbers.append(random.randint(1235, 9999))
    #return randNumbers

#def get_file_dict(args, marks):
    #fileDict = {}
    #for markname, mark in marks.iteritems():
        #try:
            #if '=' in mark:
                #fileLocation = re.search(mark+'(\S+)', ' '.join(args)).group(1)
                #fileDict[markname] = fileLocation
            #else:
                #fileLocation = args[args.index(mark) + 1]
                #fileDict[markname] = fileLocation
        #except:
            #fileDict[markname] = ''

    #print fileDict
    #return fileDict

#def run_pp_server(servers):
    #for server in servers:
        #command = "ssh {server} 'ppserver.py' &"
        #os.popen(scpCommand).read()

#def data_dispatch(fileDict, tmpLocation, servers):
    #sourceFiles = [x for x in fileDict.values() if x != '']
    #for server in servers:
        #mkdirCommand = "ssh {server} 'rm -rf {tmpLocation};mkdir {tmpLocation}'".format(
                    #server=server,
                    #tmpLocation = tmpLocation)
        #print mkdirCommand
        #os.popen(mkdirCommand).read()
        #for sourceFile in sourceFiles:
            #if 'merged' in sourceFile:
                #scpCommand = 'scp -r {sourceFile}* \
                                     #{server}:{tmpLocation}'.format(
                        #sourceFile = sourceFile,
                        #server = server,
                        #tmpLocation = tmpLocation)
            #else:
                #scpCommand = 'scp -r {sourceFile} \
                                     #{server}:{tmpLocation}'.format(
                        #sourceFile = sourceFile,
                        #server = server,
                        #tmpLocation = tmpLocation)

            #print scpCommand
            #os.popen(scpCommand).read()

    #print '======================='
    #print 'Data dispatch completed'
    #print '======================='


#def run(job):
    #os.popen(job).read()


if __name__ == '__main__':
    #probtrackx2_parallel(sys.argv[1])
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        #formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=textwrap.dedent('''
        {codeName} : Runs probtrackx in parallel
        It splits the seedmask, in the freesurfer space, into single coordinates,
        which is then used to carry out probabilistic tractography in parallel.

        It uses defaults for its probtrackx parameters.

        eg)python probtrackx2_parallel.py \\
                -s ROI/lh_thalamus.nii.gz \\
                -b DTI.bedpostX \\
                -m FREESURFER/mri/brainmask.nii.gz \\
                -r registration/FREESURFERT1toNodif.mat \\
                -o multiproc
        ========================================
        '''.format(codeName=os.path.basename(__file__))))

    parser.add_argument( '-s', '--seedmask', help='Seed mask') 
    parser.add_argument( '-b', '--bedpostxdir', help='Bedpostx directory') 
    parser.add_argument( '-m', '--fsmask', help='Freesurfer brain mask location') 
    parser.add_argument( '-r', '--regmat', help='Mask to diffusion registration matrix') 
    parser.add_argument( '-o', '--outdir', help='Output directory')
    parser.add_argument( '-j', '--j', help='Number of cores to use', default=10)

    args = parser.parse_args()

    if not args.seedmask:
        parser.print_help()
        sys.exit(0)


    probtrackx2_parallel(args)
