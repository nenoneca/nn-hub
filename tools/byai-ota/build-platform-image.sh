#!/bin/bash
# byai_platform 1.0.2 = 1.0.1 + regenerated ld.so.cache (post-slim
# deletions left the pristine-July cache pointing at removed paths —
# suspect for TIDL "Create state failed") + the libmbedcrypto.so dev
# symlink present on the board-lineage image.
set -e
SRC=${SRC:-/tmp/edgeai-orig.ext4}          # July original still on disk
WORK=/tmp/plat-build2.ext4
M=/mnt/platwork
VER=${VER:?set VER}

cp "$SRC" "$WORK"
mkdir -p $M
mount -o loop "$WORK" $M

rm -rf \
  $M/usr/lib/piglit $M/usr/lib/vulkan-cts $M/usr/lib/opengl-es-cts \
  $M/opt/ltp $M/usr/lib/rustlib $M/usr/lib/go \
  $M/opt/edgeai-test-data $M/opt/oob-demo-assets $M/tmp/* 2>/dev/null || true
find $M/usr/lib -maxdepth 1 -name "libQt6*.a" -delete
find $M/opt/model_zoo -mindepth 1 -maxdepth 1 \
  ! -name "ONR-OD-8220-yolox-s-lite-mmdet-coco-640x640" -exec rm -rf {} +
rm -rf $M/opt/nn && mkdir -p $M/opt/nn
echo "$VER" > $M/etc/nn-platform-version

# the two fixes
[ -e $M/usr/lib/libmbedcrypto.so ] || \
  ln -s "$(basename $(ls $M/usr/lib/libmbedcrypto.so.* | head -1))" $M/usr/lib/libmbedcrypto.so
chroot $M /sbin/ldconfig || chroot $M ldconfig
echo "cache regenerated: $(ls -la $M/etc/ld.so.cache | awk '{print $5}') bytes"

umount $M
e2fsck -fy "$WORK" >/dev/null 2>&1 || true
resize2fs "$WORK" 4G >/dev/null
truncate -s 4G "$WORK"
e2fsck -fy "$WORK" >/dev/null 2>&1; echo "fsck rc=$?"
gzip -1 -c "$WORK" > /tmp/plat-$VER.gz
rm -f "$WORK"
sha256sum /tmp/plat-$VER.gz; stat -c%s /tmp/plat-$VER.gz
echo PLATFORM-BUILD-DONE
