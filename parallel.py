# Tai Sakuma <tai.sakuma@cern.ch>
import alphatwirl
from alphatwirl.misc.deprecation import atdeprecated

##__________________________________________________________________||
@atdeprecated(msg='use alphatwirl.parallel.build_parallel() instead.')
def build_parallel(parallel_mode, quiet=True, processes=4, user_modules=[ ],
                   htcondor_job_desc_extra=[ ]):

    return alphatwirl.parallel.build_parallel(
        parallel_mode=parallel_mode,
        quiet=quiet,
        processes=processes,
        user_modules=user_modules,
        htcondor_job_desc_extra=htcondor_job_desc_extra
    )

##__________________________________________________________________||
