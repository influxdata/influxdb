# Linux Sysctl FS Input

The linux_sysctl_fs input provides Linux system level file metrics. The documentation on these fields can be found at https://www.kernel.org/doc/Documentation/sysctl/fs.txt.

Example output:

```
> linux_sysctl_fs,host=foo dentry-want-pages=0i,file-max=44222i,aio-max-nr=65536i,inode-preshrink-nr=0i,dentry-nr=64340i,dentry-unused-nr=55274i,file-nr=1568i,aio-nr=0i,inode-nr=35952i,inode-free-nr=12957i,dentry-age-limit=45i 1490982022000000000
```
