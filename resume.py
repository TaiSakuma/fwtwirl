#!/usr/bin/env python
# Tai Sakuma <tai.sakuma@gmail.com>
import os, sys
import argparse
import tarfile
import gzip

try:
    import cPickle as pickle
except:
    import pickle

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument('path', nargs=1, help='path to pickle')
args = parser.parse_args()

##__________________________________________________________________||
def main():

    cwd = os.getcwd()
    workingarea_path = os.path.dirname(args.path[0])
    pickle_basename = os.path.basename(args.path[0])

    setup(cwd, workingarea_path)

    run(cwd, workingarea_path, pickle_basename)

##__________________________________________________________________||
def run(cwd, workingarea_path, pickle_basename):

    os.chdir(workingarea_path)

    with gzip.open(pickle_basename, 'rb') as f:
        reader = pickle.load(f)

    os.chdir(cwd)
    reader.end()

##__________________________________________________________________||
def setup(cwd, workingarea_path):

    os.chdir(workingarea_path)

    dirname = 'python_modules'
    tarname = dirname + '.tar.gz'

    if os.path.exists(tarname) and not os.path.exists(dirname):
        if try_make_file('.untarring'):
            tar = tarfile.open(tarname)
            tar.extractall()
            tar.close()
            os.remove('.untarring')

    while os.path.isfile('.untarring'):
       pass

    if os.path.exists(dirname):
        sys.path.insert(0, dirname)

    os.chdir(cwd)

##__________________________________________________________________||
if __name__ == '__main__':
    main()
