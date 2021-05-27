#!/usr/bin/env python3

"""
ito_backup.py

Python script to copy files from an RSYNC host to disk.
Settings are pulled from config.ini.
Backups are rotated and hard links are used to save space for unchanged files.
"""

import configparser
import datetime
import locale
import logging
import os
import platform
import re
import shutil
import smtplib
import subprocess
import sys
from email.mime.text import MIMEText


class RsyncError(Exception):
    """Custom exception."""
    pass


def _mb(value):
    """Convert value to human readable megabytes."""
    locale.setlocale(locale.LC_ALL, 'en_US.utf8')
    return f"{locale.format_string(' % .2f', value / 1048576.0, True)}MB"


def _email_log(config, logfile_name, happy):
    """Email the log for this backup."""
    smtp_enable = config.get('General', 'smtp_enable')
    smtp_server = config.get('General', 'smtp_server')
    smtp_email = config.get('General', 'smtp_email')
    smtp_password = config.get('General', 'smtp_password')
    smtp_recipients = config.get('General', 'smtp_recipients')

    if smtp_enable.lower() == 'true':

        with open(logfile_name, 'rt') as fp:
            msg = fp.read()
        mime = MIMEText(msg)
        mime['From'] = smtp_email
        if happy:
            mime['Subject'] = f'Backup complete on {thismachine}'
        else:
            mime['Subject'] = f'BACKUP FAILED ON {thismachine}'
        mime['To'] = smtp_recipients
        server = smtplib.SMTP(smtp_server)
        server.login(smtp_email, smtp_password)
        server.sendmail(mime['From'], mime['To'].split(','), mime.as_string())
        server.quit()


def _timestamp():
    """Return the current date and time in a log friendly format."""
    return datetime.datetime.now().strftime("%Y-%m-%d-%H%M.%S")


# Regular expression to extract the backup byte count
SIZE_RE = re.compile(r"""total size is (\d+)""", re.MULTILINE)

LOG_LEVEL = logging.DEBUG
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
logfile_name = os.path.join('logs', f'backup-{_timestamp()}.log')

file = logging.FileHandler(logfile_name)
fileformat = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file.setLevel(LOG_LEVEL)
file.setFormatter(fileformat)

console = logging.StreamHandler()
streamformat = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console.setLevel(logging.DEBUG)
console.setFormatter(streamformat)

log.addHandler(file)
log.addHandler(console)


if __name__ == '__main__':

    thismachine = platform.node()

    #
    #   Read config.ini
    #
    config = configparser.ConfigParser()
    config.read('config.ini')

    #
    #  Batch Backup
    #
    backup_folder = config.get('General', 'backup_folder')

    #
    #   Test if the backup volume is not mounted, exit if not
    #
    if config.get('General', 'mount_check').lower() == 'true':
        if not os.path.ismount(backup_folder):
            log.error(f'!! backup point {backup_folder} not mounted')
            _email_log(config, logfile_name, False)
            sys.exit(1)

    total = 0
    happy = True

    for job in config.sections():
        if job == 'General':
            continue
        log.info(f'*** Job "{job}" begun ***')
        host = config.get(job, 'host')
        username = config.get(job, 'username')
        password = config.get(job, 'password')
        rotate_level = int(config.get(job, 'rotate_level'))
        rsync_server = f'{username}@{host.strip()}::'

        #
        #   Ask the RSYNC/Delta Copy Server what virtual directories it serves.
        #
        log.info('Requesting rsync targets from %s' % host)
        try:
            cmd = ['rsync', rsync_server]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       env={'RSYNC_PASSWORD': password})
            stdout, stderr = process.communicate()
            status = process.returncode
            if status != 0:
                raise RsyncError(stderr)
            else:
                folders = stdout.split()
        except Exception as err:
            happy = False
            err_msg = str(err)
            log.error(f'Directory listing error: {err_msg}')
            continue

        subtotal = 0

        for bytecoded_folder in folders:
            folder = bytecoded_folder.decode('utf-8')
            target_path = os.path.join(backup_folder, job)
            source = rsync_server + os.path.join(folder) + '/'
            log.info(f'Syncing "{folder}" to {target_path}')

            try:

                #
                #   Sanity Check backup folder exists and, if necessary,
                #   create a subfolder named after job.
                #
                if not os.path.isdir(backup_folder):
                    raise RsyncError(
                        f'!! backup_folder does not exist: {backup_folder}')
                if not os.path.isdir(target_path):
                    os.makedirs(target_path)

                #
                #   Purge backup sets older than rotate_level.
                #   We overshoot to 99 in case the rotation is lowered at a
                #   future date
                #
                for r in range(rotate_level-1, 99):
                    last_rot = os.path.join(target_path, f'{folder}.{r}')
                    if os.path.isdir(last_rot):
                        shutil.rmtree(last_rot)

                #
                #   Rotate the incremental backup folders upward, if they exist
                #
                for x in range(rotate_level - 2, -1, -1):
                    rot_from = os.path.join(target_path, f'{folder}.{x}')
                    rot_to = os.path.join(
                        target_path, f'{folder}.{x+1}')
                    if os.path.isdir(rot_from):
                        os.rename(rot_from, rot_to)

                #
                #   Create the new rsync target folder.0
                #
                zero_folder_path = os.path.join(target_path, f'{folder}.0')
                os.mkdir(zero_folder_path)

                #
                #   Start building the parameters for the call to rsync
                #
                parms = ['-rav', '--delete', '--no-perms', '--chmod=ugo=rwX',
                         '--no-super', '--no-group', '--no-human-readable']

                #
                #   If we have last night's backup, pass it as rsync's link
                #   destination to preserve disk space
                #
                link_src = os.path.join(target_path, f'{folder}.1')
                if os.path.isdir(link_src):
                    parms.append(f'--link-dest={link_src}')
                parms.append(source)
                parms.append(zero_folder_path)
                cmd = ['rsync']
                cmd += parms
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE,
                                           env={"RSYNC_PASSWORD": password})
                stdout, stderr = process.communicate()
                status = process.returncode
                if status != 0:
                    raise RsyncError(stderr)
                else:
                    match_obj = SIZE_RE.search(stdout.decode('utf-8'))
                    size = int(match_obj.group(1))
                    subtotal += size
                    log.info(f'Synchronized {_mb(size)}')

            except Exception as err:
                happy = False
                err_msg = str(err)
                log.error(f'!! Error: {err_msg}')

        log.info(f'Job {job} complete with {_mb(subtotal)} processed')
        total += subtotal

    log.info(
        f'Batch backup finished on "{thismachine}". '
        f'Total size was {_mb(total)}')
    _email_log(config, logfile_name, happy)
