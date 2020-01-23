#!/usr/bin/sh
cryptsetup luksOpen /dev/md0 encrypted-md0
mount /dev/mapper/encrypted-md0 /backup
