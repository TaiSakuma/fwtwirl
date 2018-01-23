# Tai Sakuma <tai.sakuma@cern.ch>
import os
import sys
import logging

import alphatwirl

from parallel import build_parallel
from profile_func import profile_func

##__________________________________________________________________||
class FrameworkDelphes(object):
    def __init__(self,
                 quiet = False,
                 parallel_mode = 'multiprocessing',
                 htcondor_job_desc_extra = [ ],
                 process = 8,
                 user_modules = (),
                 max_events_per_dataset = -1,
                 max_events_per_process = -1,
                 max_files_per_dataset = -1,
                 max_files_per_process = 1,
                 profile = False,
                 profile_out_path = None
    ):
        user_modules = set(user_modules)
        self.parallel = build_parallel(
            parallel_mode = parallel_mode,
            quiet = quiet,
            processes = process,
            user_modules = user_modules,
            htcondor_job_desc_extra = htcondor_job_desc_extra,
        )
        self.max_events_per_dataset = max_events_per_dataset
        self.max_events_per_process = max_events_per_process
        self.max_files_per_dataset = max_files_per_dataset
        self.max_files_per_process = max_files_per_process
        self.profile = profile
        self.profile_out_path = profile_out_path

    def run(self, datasets, reader_collector_pairs):
        self._begin()
        try:
            loop = self._configure(datasets, reader_collector_pairs)
            self._run(loop)
        except KeyboardInterrupt:
            logger = logging.getLogger(__name__)
            logger.warning('received KeyboardInterrupt')
            pass
        self._end()

    def _begin(self):
        self.parallel.begin()

    def _configure(self, datasets, reader_collector_pairs):
        reader_top = alphatwirl.loop.ReaderComposite()
        collector_top = alphatwirl.loop.CollectorComposite(self.parallel.progressMonitor.createReporter())
        for r, c in reader_collector_pairs:
            reader_top.add(r)
            collector_top.add(c)
        eventLoopRunner = alphatwirl.loop.MPEventLoopRunner(self.parallel.communicationChannel)
        eventBuilderConfigMaker = alphatwirl.delphes.EventBuilderConfigMaker()
        datasetIntoEventBuildersSplitter = alphatwirl.loop.DatasetIntoEventBuildersSplitter(
            EventBuilder = alphatwirl.delphes.DelphesEventBuilder,
            eventBuilderConfigMaker = eventBuilderConfigMaker,
            maxEvents = self.max_events_per_dataset,
            maxEventsPerRun = self.max_events_per_process,
            maxFiles = self.max_files_per_dataset,
            maxFilesPerRun = self.max_files_per_process
        )
        eventReader = alphatwirl.loop.EventsInDatasetReader(
            eventLoopRunner = eventLoopRunner,
            reader = reader_top,
            collector = collector_top,
            split_into_build_events = datasetIntoEventBuildersSplitter
        )
        loop = DatasetLoop(datasets = datasets, reader = eventReader)
        return loop

    def _run(self, loop):
        if not self.profile:
            loop()
        else:
            profile_func(func = loop, profile_out_path = self.profile_out_path)

    def _end(self):
        self.parallel.end()

##__________________________________________________________________||
class DatasetLoop(object):

    def __init__(self, datasets, reader):
        self.datasets = datasets
        self.reader = reader

    def __call__(self):
        self.reader.begin()
        for dataset in self.datasets:
            self.reader.read(dataset)
        return self.reader.end()

##__________________________________________________________________||
