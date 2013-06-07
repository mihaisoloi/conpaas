#!/bin/bash

# Customizable
DEFAULT_FREE_SPACE=64 #MB


# Function for displaying highlighted messages.
function cecho() {
  echo -en "\033[1m"
  echo -n "#" $@
  echo -e "\033[0m"
}

if [ `id -u` -ne 0 ]; then
	exec 1>&2; cecho "error: Need root permissions for this script."
 	exit 1
fi

if [ -z "$1" ] ; then
	exec 1>&2; cecho "error: No image specified."
	exit 1
fi

SRC_IMG=$1
DST_IMG=squeezed-$1


FS_SPACE=40 #MB
if [ -z "$2" ] ; then
	FREE_SPACE=$DEFAULT_FREE_SPACE
elif [[ "$2" =~ ^[0-9]+$ ]] ; then
	FREE_SPACE=$2
else
	exec 1>&2; cecho "error: The 'FREE_SPACE' argument \
			should be a number."
	exit 1
fi

apt-get -y install mount kpartx parted e2fsprogs grub2

cecho "Mounting old image."
SRC_LOOP=`losetup -f`
losetup $SRC_LOOP $SRC_IMG

PARTITION=`kpartx -l $SRC_LOOP | awk '{ print $1 }'`
PARTITION=/dev/mapper/$PARTITION
kpartx -a $SRC_LOOP

SRC_DIR=`mktemp -d`
mount -o loop $PARTITION $SRC_DIR

USED_SPACE=`(cd $SRC_DIR ; df --total --block-size=M  . | tail -n 1 | awk '{print $3}' | tr -d 'M')`

DST_SIZE=`python -c "print ($USED_SPACE + $FS_SPACE + $FREE_SPACE)"`
## Use 1GB granularity for image size  
#gb_size="1024.0"
#DST_SIZE=`python -c "import math; print int(math.ceil($DST_SIZE/$gb_size) * $gb_size)"`

cecho "Clean."	# TODO: I need to find out why this is needed and fix it.
umount $SRC_DIR
sleep 1s
kpartx -d $SRC_LOOP
sleep 1s
losetup -d $SRC_LOOP
sleep 1s
rmdir $SRC_DIR


# Create new image
dd if=/dev/zero of=$DST_IMG bs=1M count=$DST_SIZE

PART_OFFSET_IN_BYTES=`parted -s $SRC_IMG unit B print | awk \
	'/Number.*Start.*End/ { getline; print $2 }' | tr -d 'B'`
PART_OFFSET_IN_SECTORS=`parted -s $SRC_IMG unit S print | awk \
	'/Number.*Start.*End/ { getline; print $2 }' | tr -d 's'`

# Create filesystem on new image and mount it
cecho "Writing partition table."
parted -s $DST_IMG mklabel msdos

cecho "Creating primary partition."
cyl_total=`parted -s $DST_IMG unit s print | awk \
	'{if (NF > 2 && $1 == "Disk") print $0}' | sed \
	's/Disk .* \([0-9]\+\)s/\1/'`
cyl_partition=`expr $cyl_total - $PART_OFFSET_IN_SECTORS`
parted -s $DST_IMG unit s mkpart primary 2048 $cyl_partition

cecho "Setting boot flag."
parted -s $DST_IMG set 1 boot on

DST_LOOP=`losetup -f`
losetup $DST_LOOP $DST_IMG

PARTITION=`kpartx -l $DST_LOOP | awk '{ print $1 }'`
PARTITION=/dev/mapper/$PARTITION
kpartx -a $DST_LOOP

cecho "Creating ext3 filesystem."
echo 'y' | mkfs.ext3 $PARTITION
cecho "Setting label 'root'."
tune2fs $PARTITION -L root

cecho "Mounting new image."
DST_DIR=`mktemp -d`
mount -o loop $PARTITION $DST_DIR

cecho "Mounting old image."
SRC_LOOP=`losetup -f`
losetup $SRC_LOOP $SRC_IMG

PARTITION=`kpartx -l $SRC_LOOP | awk '{ print $1 }'`
PARTITION=/dev/mapper/$PARTITION
kpartx -a $SRC_LOOP

SRC_DIR=`mktemp -d`
mount -o loop $PARTITION $SRC_DIR

# Copy files
cecho "Copying files."
( cd $SRC_DIR && tar -cf - . ) | ( cd $DST_DIR && tar -xpf -)

cecho "Running grub-install"
grub-install --no-floppy --grub-mkdevicemap=$DST_DIR/boot/grub/device.map --root-directory=$DST_DIR $DST_LOOP

cecho "Clean."
umount $SRC_DIR
sleep 1s
kpartx -d $SRC_LOOP
sleep 1s
losetup -d $SRC_LOOP
sleep 1s
rmdir $SRC_DIR

umount $DST_DIR
sleep 1s
kpartx -d $DST_LOOP
sleep 1s
losetup -d $DST_LOOP
sleep 1s
rmdir $DST_DIR

cecho "Done."