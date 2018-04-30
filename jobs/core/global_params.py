#!/usr/bin/env python3

import luigi


class GlobalTrackingParams(luigi.Config):
    tracking_url = luigi.Parameter(default="")
    tracking_id = luigi.IntParameter(default=0)
