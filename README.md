# zig-xbream

This repo is a very partial re-implementation of the rust crate [crossbeam](https://github.com/crossbeam-rs/crossbeam).

Only the following types are implemented:

#### Data structures

* `xbeam.queue.ArrayQueue` - a bounded MPMC queue that allocates a fixed-capacity buffer on construction.
* `xbeam.queue.ArrayQueueNoAlloc` - an implementation of `ArrayQueue` that does not allocate.
 
#### Utilities

* `xbeam.utils.Backoff` - for exponential backoff in spin loops.
* `xbeam.utils.CACHE_LINE_LENGTH` - replacement for the rust `CachePadded`, used like this:

    ```zig 
    var aligned: usize align(xbeam.utils.CACHE_LINE_LENGTH) = 0;
    ```
 
### Contributions are welcome!

## How to use

Download the repo somehow then either:

### Add as package in `build.zig`

* To `build.zig` add:
  
   ```zig
   exe.addPackagePath("xbeam", "zig-xbeam/src/index.zig"); // or whatever the path is
   ```
* Then the package is available within any zig file:
  
   ```zig
   const xbeam = @import("xbeam");
   ```

### Import directly

In any zig file add:
```zig
const xbeam = @import("../zig-xbeam/src/index.zig"); // or whatever the path is from *that* file
```
