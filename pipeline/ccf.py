

import csv
import logging

import numpy as np
import datajoint as dj

from tifffile import imread

from pipeline.reference import ccf_ontology


log = logging.getLogger(__name__)
schema = dj.schema(dj.config.get('ccf.database', 'map_ccf'))


@schema
class CCFLabel(dj.Lookup):
    definition = """
    # CCF Dataset Information
    ccf_label_id:       int             # Local CCF ID
    ---
    ccf_version:        int             # Allen CCF Version
    ccf_resolution:     int             # Voxel Resolution (uM)
    ccf_description:    varchar(255)    # CCFLabel Description
    """
    CCF_R3_20UM_ID = 0
    CCF_R3_20UM_DESC = 'Allen Institute Mouse CCF, Rev. 3, 20uM Resolution'
    CCF_R3_20UM_TYPE = 'CCF_R3_20UM'

    contents = [
        (CCF_R3_20UM_ID, 3, 20,
         "Allen Institute Mouse CCF, Revision 3, 20 uM Resolution",)]


@schema
class CCF(dj.Lookup):
    definition = """
    # Common Coordinate Framework
    -> CCFLabel
    x   :  int   # (um)
    y   :  int   # (um)
    z   :  int   # (um)
    """


@schema
class AnnotationType(dj.Lookup):
    definition = """
    annotation_type  : varchar(16)
    """
    contents = ((CCFLabel.CCF_R3_20UM_TYPE,),)


@schema
class CCFAnnotation(dj.Manual):
    definition = """
    -> CCF
    -> AnnotationType
    ---
    annotation  : varchar(1200)
    """

    @classmethod
    def load_ccf_r3_20um(cls):
        """
        Load the CCF r3 20 uM Dataset.
        Requires that dj.config['ccf.r3_20um_path'] be set to the location
        of the CCF Annotation tif stack.
        """
        # TODO: scaling
        log.info('CCFAnnotation.load_ccf_r3_20um(): start')
        dj.conn().start_transaction()

        self = cls()  # Instantiate self,
        stack_path = dj.config['ccf.r3_20um_path']
        stack = imread(stack_path)  # load reference stack,

        log.info('.. loaded stack of shape {} from {}'
                 .format(stack.shape, stack_path))

        # iterate over ccf ontology region id/name records,
        regions = [c for c in csv.reader(ccf_ontology.splitlines())
                   if len(c) == 2]

        region = 0
        nregions = len(regions)
        for num, txt in regions:

            region += 1
            num = int(num)

            log.info('.. loading region {} ({}/{}) ({})'
                     .format(num, region, nregions, txt))

            # extracting filled volumes from stack in scaled [[x,y,z]] shape,
            vol = np.array(np.where(stack == num)).T[:, [1, 2, 0]] * 20

            if not vol.shape[0]:
                log.info('.. region {} volume: shape {} - skipping'
                         .format(num, vol.shape))
                continue

            log.info('.. region {} volume: shape {}'.format(num, vol.shape))

            # creating corresponding base CCF records if necessary,
            CCF.insert(((CCFLabel.CCF_R3_20UM_ID, *vox) for vox in vol),
                       skip_duplicates=True)

            # and adding to the annotation set.
            self.insert(((CCFLabel.CCF_R3_20UM_ID, *vox,
                         CCFLabel.CCF_R3_20UM_TYPE, txt) for vox in vol),
                        skip_duplicates=True)

        dj.conn().commit_transaction()
        log.info('.. done.')
