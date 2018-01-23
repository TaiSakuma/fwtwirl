# Tai Sakuma <tai.sakuma@cern.ch>
import logging

import alphatwirl

##__________________________________________________________________||
class Parallel(object):
    def __init__(self, progressMonitor, communicationChannel):
        self.progressMonitor = progressMonitor
        self.communicationChannel = communicationChannel

    def __repr__(self):
        return '{}(progressMonitor={!r}, communicationChannel={!r}'.format(
            self.__class__.__name__,
            self.progressMonitor,
            self.communicationChannel
        )

    def begin(self):
        self.progressMonitor.begin()
        self.communicationChannel.begin()

    def end(self):
        self.progressMonitor.end()
        self.communicationChannel.end()

##__________________________________________________________________||
def build_parallel(parallel_mode, quiet=True, processes=4, user_modules=[ ], htcondor_job_desc_extra=[ ]):

    default_parallel_mode = 'multiprocessing'

    if parallel_mode in ('subprocess', 'htcondor'):
        return build_parallel_dropbox(
            parallel_mode=parallel_mode,
            quiet=quiet,
            user_modules=user_modules,
            htcondor_job_desc_extra=htcondor_job_desc_extra
        )

    if not parallel_mode == default_parallel_mode:
        logger = logging.getLogger(__name__)
        logger.warning('unknown parallel_mode "{}", use default "{}"'.format(
            parallel_mode, default_parallel_mode
        ))

    return build_parallel_multiprocessing(quiet=quiet, processes=processes)

##__________________________________________________________________||
def build_parallel_dropbox(parallel_mode, quiet, user_modules, htcondor_job_desc_extra=[ ]):
    tmpdir = '_ccsp_temp'
    user_modules = set(user_modules)
    user_modules.add('fwtwirl')
    user_modules.add('alphatwirl')
    alphatwirl.mkdir_p(tmpdir)
    progressMonitor = alphatwirl.progressbar.NullProgressMonitor()
    if parallel_mode == 'htcondor':
        dispatcher = alphatwirl.concurrently.HTCondorJobSubmitter(job_desc_extra=htcondor_job_desc_extra)
    else:
        dispatcher = alphatwirl.concurrently.SubprocessRunner()
    workingArea = alphatwirl.concurrently.WorkingArea(
        dir=tmpdir,
        python_modules=list(user_modules)
    )
    dropbox = alphatwirl.concurrently.TaskPackageDropbox(
        workingArea=workingArea,
        dispatcher=dispatcher
    )
    communicationChannel = alphatwirl.concurrently.CommunicationChannel(
        dropbox=dropbox
    )
    return Parallel(progressMonitor, communicationChannel)

##__________________________________________________________________||
def build_parallel_multiprocessing(quiet, processes):
    progressMonitor, communicationChannel = alphatwirl.configure.build_progressMonitor_communicationChannel(quiet=quiet, processes=processes)
    return Parallel(progressMonitor, communicationChannel)

##__________________________________________________________________||
