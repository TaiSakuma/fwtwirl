# Tai Sakuma <tai.sakuma@cern.ch>
import os
import sys
import logging

import alphatwirl

##__________________________________________________________________||
import logging
logger = logging.getLogger(__name__)
log_handler = logging.StreamHandler(stream=sys.stdout)
log_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

##__________________________________________________________________||
from parallel import build_parallel
from profile_func import profile_func

##__________________________________________________________________||
class FrameworkHeppy(object):
    """A simple framework for using alphatwirl

    Args:
        outdir (str): the output directory
        datamc (str): 'data' or 'mc'
        force (bool): overwrite the output if True
        quiet (bool): don't show progress bars if True
        parallel_mode (str): 'multiprocessing', 'subprocess', 'htcondor'
        process (int): the number of processes for the 'multiprocessing' mode
        user_modules (list of str): names of python modules to be copied for the 'subprocess' mode
        max_events_per_dataset (int):
        max_events_per_process (int):
        profile (bool): run cProfile if True
        profile_out_path (bool): path to store the result of the profile. stdout if None

    """
    def __init__(self, outdir, heppydir,
                 datamc='mc',
                 force=False, quiet=False,
                 parallel_mode='multiprocessing',
                 htcondor_job_desc_extra=[ ],
                 process=8,
                 user_modules=(),
                 max_events_per_dataset=-1, max_events_per_process=-1,
                 profile=False, profile_out_path=None,
                 keep_jobs_running_at_keyboardinterrupt=True
    ):
        self.parallel = build_parallel(
            parallel_mode=parallel_mode,
            quiet=quiet,
            processes=process,
            user_modules=user_modules,
            htcondor_job_desc_extra=htcondor_job_desc_extra,
            terminate_dispatcher_at_close=keep_jobs_running_at_keyboardinterrupt
        )
        self.outdir = outdir
        self.heppydir = heppydir
        self.datamc = datamc
        self.force =  force
        self.max_events_per_dataset = max_events_per_dataset
        self.max_events_per_process = max_events_per_process
        self.profile = profile
        self.profile_out_path = profile_out_path

    def run(self, components,
            reader_collector_pairs,
            analyzerName='roctree',
            fileName='tree.root',
            treeName='tree'
    ):

        self._begin()
        try:
            loop = self._configure(components, reader_collector_pairs, analyzerName, fileName, treeName)
            self._run(loop)
        except KeyboardInterrupt:
            logger = logging.getLogger(__name__)
            logger.warning('received KeyboardInterrupt')
            pass
        self._end()

    def _begin(self):
        self.parallel.begin()

    def _configure(self, components, reader_collector_pairs, analyzerName, fileName, treeName):

        component_readers = alphatwirl.heppyresult.ComponentReaderComposite()


        # tbl_heppyresult.txt
        tbl_heppyresult_path = os.path.join(self.outdir, 'tbl_heppyresult.txt')
        if self.force or not os.path.exists(tbl_heppyresult_path):
            # e.g., '74X/MC/20150810_MC/20150810_SingleMu'
            heppydir_rel = '/'.join(self.heppydir.rstrip('/').split('/')[-4:])
            alphatwirl.mkdir_p(os.path.dirname(tbl_heppyresult_path))
            f = open(tbl_heppyresult_path, 'w')
            f.write('heppyresult\n')
            f.write(heppydir_rel + '\n')
            f.close()

        # tbl_tree.txt
        tbl_tree_path = os.path.join(self.outdir, 'tbl_tree.txt')
        if self.force or not os.path.exists(tbl_tree_path):
            tblTree = alphatwirl.heppyresult.TblTree(
                analyzerName=analyzerName,
                fileName=fileName,
                treeName=treeName,
                outPath=tbl_tree_path,
            )
            component_readers.add(tblTree)

        # tbl_branch.txt
        tbl_branch_path = os.path.join(self.outdir, 'tbl_branch.txt')
        if self.force or not os.path.exists(tbl_branch_path):
            tblBranch = alphatwirl.heppyresult.TblBranch(
                analyzerName=analyzerName,
                fileName=fileName,
                treeName=treeName,
                outPath=tbl_branch_path,
            )
            component_readers.add(tblBranch)

        # tbl_branch_size.tx
        tbl_branch_size_path = os.path.join(self.outdir, 'tbl_branch_size.txt')
        if self.force or not os.path.exists(tbl_branch_size_path):
            tblBranchSize = alphatwirl.heppyresult.TblBranch(
                analyzerName=analyzerName,
                fileName=fileName,
                treeName=treeName,
                outPath=tbl_branch_size_path,
                addType=False,
                addSize=True,
                sortBySize=True,
            )
            component_readers.add(tblBranchSize)

        # tbl_branch_title.txt
        tbl_branch_title_path = os.path.join(self.outdir, 'tbl_branch_title.txt')
        if self.force or not os.path.exists(tbl_branch_title_path):
            tblBranchTitle = alphatwirl.heppyresult.TblBranch(
                analyzerName=analyzerName,
                fileName=fileName,
                treeName=treeName,
                outPath=tbl_branch_title_path,
                addType=False,
                addSize=False,
                addTitle=True,
            )
            component_readers.add(tblBranchTitle)

        # tbl_dataset.txt
        tbl_dataset_path = os.path.join(self.outdir, 'tbl_dataset.txt')
        if self.force or not os.path.exists(tbl_dataset_path):
            tblDataset = alphatwirl.heppyresult.TblComponentConfig(
                outPath=tbl_dataset_path,
                columnNames=('dataset', ),
                keys=('dataset', ),
            )
            component_readers.add(tblDataset)

        # tbl_xsec.txt for MC
        if self.datamc == 'mc':
            tbl_xsec_path = os.path.join(self.outdir, 'tbl_xsec.txt')
            if self.force or not os.path.exists(tbl_xsec_path):
                tblXsec = alphatwirl.heppyresult.TblComponentConfig(
                    outPath=tbl_xsec_path,
                    columnNames=('xsec', ),
                    keys=('xSection', ),
                )
                component_readers.add(tblXsec)

        # tbl_nevt.txt for MC
        if self.datamc == 'mc':
            tbl_nevt_path = os.path.join(self.outdir, 'tbl_nevt.txt')
            if self.force or not os.path.exists(tbl_nevt_path):
                tblNevt = alphatwirl.heppyresult.TblCounter(
                    outPath=tbl_nevt_path,
                    columnNames=('nevt', 'nevt_sumw'),
                    analyzerName='skimAnalyzerCount',
                    fileName='SkimReport.txt',
                    levels=('All Events', 'Sum Weights')
                )
                component_readers.add(tblNevt)

        # event loop
        reader = alphatwirl.loop.ReaderComposite()
        collector = alphatwirl.loop.CollectorComposite(self.parallel.progressMonitor.createReporter())
        for r, c in reader_collector_pairs:
            reader.add(r)
            collector.add(c)
        eventLoopRunner = alphatwirl.loop.MPEventLoopRunner(self.parallel.communicationChannel)
        eventBuilderConfigMaker = alphatwirl.heppyresult.EventBuilderConfigMaker(
            analyzerName=analyzerName,
            fileName=fileName,
            treeName=treeName,
        )
        datasetIntoEventBuildersSplitter = alphatwirl.loop.DatasetIntoEventBuildersSplitter(
            EventBuilder=alphatwirl.heppyresult.EventBuilder,
            eventBuilderConfigMaker=eventBuilderConfigMaker,
            maxEvents=self.max_events_per_dataset,
            maxEventsPerRun=self.max_events_per_process
        )
        eventReader = alphatwirl.loop.EventsInDatasetReader(
            eventLoopRunner=eventLoopRunner,
            reader=reader,
            collector=collector,
            split_into_build_events=datasetIntoEventBuildersSplitter
        )
        component_readers.add(eventReader)

        if components == ['all']: components = None
        heppyResult = alphatwirl.heppyresult.HeppyResult(
            path=self.heppydir,
            componentNames=components,
            componentHasTheseFiles=[analyzerName]
        )
        componentLoop = alphatwirl.heppyresult.ComponentLoop(heppyResult, component_readers)

        return componentLoop

    def _run(self, componentLoop):
        if not self.profile:
            componentLoop()
        else:
            profile_func(func=componentLoop, profile_out_path=self.profile_out_path)

    def _end(self):
        self.parallel.end()

##__________________________________________________________________||
