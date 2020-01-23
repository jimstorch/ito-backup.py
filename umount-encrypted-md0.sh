#!/usr/bin/sh
umount /backup
cryptsetup luksClose encrypted-md0
