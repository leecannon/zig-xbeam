/// a bounded MPMC queue that uses a fixed-capacity buffer.
/// a no allocation version of `ArrayQueue`
pub const ArrayQueueNoAlloc = @import("queue/array_queue_no_alloc.zig").ArrayQueueNoAlloc;

/// a bounded MPMC queue that allocates a fixed-capacity buffer on construction.
pub const ArrayQueue = @import("queue/array_queue.zig").ArrayQueue;

comptime {
    @import("std").testing.refAllDecls(@This());
}
