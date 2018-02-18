# Tai Sakuma <tai.sakuma@cern.ch>
from alphatwirl.misc import print_profile_func
from alphatwirl.misc.deprecation import atdeprecated

##__________________________________________________________________||
@atdeprecated(msg='use alphatwirl.misc.print_profile_func() instead.')
def profile_func(func, profile_out_path=None):
    print_profile_func(func, profile_out_path)

##__________________________________________________________________||
