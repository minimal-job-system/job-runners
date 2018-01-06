#!/usr/bin/env python

import luigi


from observables import ObservableJobSystemWorkflow


class JobSystemWorkflow(luigi.WrapperTask):
    """
    Default luigi workflow task.
    """
    pass
