/*
 * rproc_byname — resolve /dev/remoteprocN by CORE NAME, not by index.
 *
 * TI's libtivision_apps.so hardcodes open("/dev/remoteproc0") to attach
 * its dma-bufs to the C7x.  remoteproc indices are handed out in driver
 * probe order, which is a udev race: on 2026-08-19 the offline MCU R5F
 * won index 0 and the 10.9 MB TIDL network buffer bounced through
 * swiotlb (256 KB per-mapping cap) and failed — nn-camera segfault-
 * looped.  The library is prebuilt, so the fix is this LD_PRELOAD shim:
 * every open of /dev/remoteprocN is redirected to whichever
 * /sys/class/remoteproc entry NAMES the core the caller actually means.
 *
 * Mapping comes from NN_RPROC_MAP ("0=7e000000.dsp,1=7e200000.dsp" by
 * default).  A core that cannot be found by name falls through to the
 * original path, so the shim can only ever improve on the status quo.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <pthread.h>

#define MAX_MAP 8
static struct { int idx; char name[64]; char dev[32]; int resolved; } map[MAX_MAP];
static int nmap = 0;
static pthread_once_t once = PTHREAD_ONCE_INIT;

static void build_map(void)
{
    const char *spec = getenv("NN_RPROC_MAP");
    if (!spec || !*spec)
        spec = "0=7e000000.dsp,1=7e200000.dsp";
    char buf[256];
    snprintf(buf, sizeof buf, "%s", spec);
    for (char *sav, *tok = strtok_r(buf, ",", &sav);
         tok && nmap < MAX_MAP; tok = strtok_r(NULL, ",", &sav)) {
        char *eq = strchr(tok, '=');
        if (!eq) continue;
        *eq = 0;
        map[nmap].idx = atoi(tok);
        snprintf(map[nmap].name, sizeof map[nmap].name, "%s", eq + 1);
        nmap++;
    }
    /* Resolve each wanted core name to today's index by scanning sysfs. */
    for (int i = 0; i < nmap; i++) {
        for (int n = 0; n < 16; n++) {
            char p[80], nm[64] = "";
            snprintf(p, sizeof p, "/sys/class/remoteproc/remoteproc%d/name", n);
            FILE *f = fopen(p, "re");
            if (!f) continue;
            if (fgets(nm, sizeof nm, f))
                nm[strcspn(nm, "\n")] = 0;
            fclose(f);
            if (!strcmp(nm, map[i].name)) {
                snprintf(map[i].dev, sizeof map[i].dev, "/dev/remoteproc%d", n);
                map[i].resolved = 1;
                break;
            }
        }
        fprintf(stderr, "rproc_byname: /dev/remoteproc%d (%s) -> %s\n",
                map[i].idx, map[i].name,
                map[i].resolved ? map[i].dev : "NOT FOUND, passing through");
    }
}

static const char *redirect(const char *path)
{
    int idx;
    if (!path || sscanf(path, "/dev/remoteproc%d", &idx) != 1)
        return path;
    pthread_once(&once, build_map);
    for (int i = 0; i < nmap; i++)
        if (map[i].idx == idx && map[i].resolved)
            return map[i].dev;
    return path;
}

#define WRAP(fn)                                                          \
int fn(const char *path, int flags, ...)                                  \
{                                                                         \
    static int (*real)(const char *, int, ...);                           \
    if (!real) real = dlsym(RTLD_NEXT, #fn);                              \
    mode_t mode = 0;                                                      \
    if (flags & O_CREAT) {                                                \
        va_list ap; va_start(ap, flags);                                  \
        mode = va_arg(ap, mode_t); va_end(ap);                            \
    }                                                                     \
    return real(redirect(path), flags, mode);                             \
}
WRAP(open)
WRAP(open64)

#define WRAPAT(fn)                                                        \
int fn(int dirfd, const char *path, int flags, ...)                       \
{                                                                         \
    static int (*real)(int, const char *, int, ...);                      \
    if (!real) real = dlsym(RTLD_NEXT, #fn);                              \
    mode_t mode = 0;                                                      \
    if (flags & O_CREAT) {                                                \
        va_list ap; va_start(ap, flags);                                  \
        mode = va_arg(ap, mode_t); va_end(ap);                            \
    }                                                                     \
    return real(dirfd, redirect(path), flags, mode);                      \
}
WRAPAT(openat)
WRAPAT(openat64)
