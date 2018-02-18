#!/usr/bin/env python3

import luigi


class GlobalLuigiParams(luigi.Config):
    workers = luigi.IntParameter(default=1)


class GlobalTrackingParams(luigi.Config):
    tracking_url = luigi.Parameter(default="")
    tracking_id = luigi.IntParameter(default=0)
