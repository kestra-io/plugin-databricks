#!/bin/sh
# Hands the task working directory to the unprivileged `kestra` user, then runs the command as that user.
# Kestra's Docker task runner creates the working directory as root, so UID 1000 cannot write to it otherwise.
set -e

if [ "$(id -u)" != "0" ]; then
    exec "$@"
fi

workdir="$(pwd -P)"
# Never recursively chown the filesystem root or the image's own directories.
case "$workdir" in
    /|/usr|/usr/*|/bin|/sbin|/lib|/lib/*|/etc|/etc/*|/var|/var/*|/opt|/opt/*|/proc|/proc/*|/sys|/sys/*|/dev|/dev/*|/root|/root/*)
        ;;
    *)
        chown -R --no-dereference kestra:kestra "$workdir" \
            || echo "kestra-entrypoint: could not chown $workdir, the task may fail to write there" >&2
        ;;
esac

export HOME=/home/kestra USER=kestra LOGNAME=kestra
exec setpriv --reuid=1000 --regid=1000 --init-groups "$@"
