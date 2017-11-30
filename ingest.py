#! /usr/bin/env python

import os
import sys

import scipy.io as spio

import datajoint as dj

import experiment


if 'imported_session_path' not in dj.config:
    dj.config['imported_session_path'] = './data'

schema = dj.schema(dj.config['ingest.database'], locals())


def _listfiles():
    return (f for f in os.listdir(dj.config['imported_session_path'])
            if f.endswith('.mat'))


@schema
class ImportedSessionFile(dj.Lookup):
    # TODO: more representative class name
    definition = """
    imported_session_file:         varchar(255)    # imported session file
    """

    contents = ((f,) for f in (_listfiles()))

    def populate(self):
        for f in _listfiles():
            if not self & {'imported_session_file': f}:
                self.insert1((f,))


@schema
class ImportedSessionFileIngest(dj.Imported):
    definition = """
    -> ImportedSessionFile
    ---
    -> experiment.Session
    """

    @property
    def key_source(self):
        return ImportedSessionFile()

    def make(self, key):

        sfname = key['imported_session_file']
        sfpath = os.path.join(dj.config['imported_session_path'], sfname)

        print('ImportedSessionFileIngest.make(): Loading {f}'.format(f=sfname))

        mat = spio.loadmat(sfpath, squeeze_me=True)
        SessionData = mat['SessionData']

        # construct session key & add session
        #
        # HACK:
        #
        # Session.session as designed was to be Nth session per animal;
        # assuming here all sessions are for 1 animal and taking
        # number-of-sessions-for-animal+1 as the key..
        # best would be to find animal & trial deterministically
        # from original ingest...
        #

        skey = {'animal': 399572}
        if not experiment.Session() & skey:
            skey['session'] = 1
        else:
            skey['session'] = len(experiment.Session() & skey) + 1

        if experiment.Session() & skey:
            # XXX: raise DataJointError?
            print("Warning! session exists for {f}".format(sfname),
                  file=sys.stderr)

        # do rest of data loading here
        # ...
        # and save a record here to prevent future loading
        # self.insert1(dict(**key, **skey))


if __name__ == '__main__':
    if len(sys.argv) < 2 or sys.argv[1] != 'populate':
        print("usage: {p} [populate]"
              .format(p=os.path.basename(sys.argv[0])))
        sys.exit(0)

    ImportedSessionFile().populate()
    ImportedSessionFileIngest().populate()
