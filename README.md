# Job-Runners
Runners for the Minimal Job System.

## Luigi Runner
### Service Overview

| File            | Description                             | Location*                         |
| --------------- | --------------------------------------- | --------------------------------- |
| lgrunnerd.py    | service program                         | /usr/local/bin                    |
| lgrunnerd       | startup script for the service          | /etc/init.d                       |
| lgrunnerd.conf  | configuration file of the service       | /etc/lgrunner.d                   |
| luigi.conf      | configuration file of the luigi library | /etc/lgrunner.d                   |
| lgrunnerd.pid** | PID file of the service                 | /var/run/user/{user_id}/lgrunnerd |
| lgrunnerd.log** | log file of the service                 | /var/log/lgrunnerd/               |
| luigi.log**     | log file of the luigi library           | /var/log/lgrunnerd/               |


\* recommended for CentOS 7
\** automatically generated

### Start the service with the system
Create symlink for each runlevel which needs this service running, e.g. /etc/init.d/rc5.d/S99lgrunnerd.

### Dependencies
- Luigi Daemon (luigid) [https://github.com/spotify/luigi]
- Minimal Job System [https://github.com/minimal-job-system/minimal-job-system]
