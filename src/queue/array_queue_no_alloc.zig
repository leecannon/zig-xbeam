//! The implementation is based on Dmitry Vyukov's bounded MPMC queue.
//!
//! Source:
//!   - <http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue>

const xbeam = @import("../index.zig");
const std = @import("std");

const AtomicUsize = std.atomic.Int(usize);

/// A bounded multi-producer multi-consumer queue.
///
/// This queue contains a fixed-capacity buffer, which is used to store pushed elements.
/// The queue cannot hold more elements than the buffer allows. Attempting to push an
/// element into a full queue will fail.
pub fn ArrayQueueNoAlloc(comptime T: type, comptime size: usize) type {
    if (comptime size == 0) @compileError("size must be non-zero");

    // One lap is the smallest power of two greater than `cap`
    const one_lap: usize = std.math.ceilPowerOfTwo(usize, size + 1) catch @compileError("unable to find next power of two");

    return struct {
        const Self = @This();

        /// A slot in a queue.
        const Slot = struct {
            /// The current stamp.
            ///
            /// If the stamp equals the tail, this node will be next written to. If it equals head + 1,
            /// this node will be next read from.
            stamp: AtomicUsize,

            /// The value in this slot.
            value: T = undefined,
        };

        pub const CAPACITY: usize = size;

        /// The head of the queue.
        ///
        /// This value is a "stamp" consisting of an index into the buffer and a lap, but packed into a
        /// single `usize`. The lower bits represent the index, while the upper bits represent the lap.
        ///
        /// Elements are popped from the head of the queue.
        head: AtomicUsize align(xbeam.utils.CACHE_LINE_LENGTH) = AtomicUsize.init(0),

        /// The tail of the queue.
        ///
        /// This value is a "stamp" consisting of an index into the buffer and a lap, but packed into a
        /// single `usize`. The lower bits represent the index, while the upper bits represent the lap.
        ///
        /// Elements are pushed into the tail of the queue.
        tail: AtomicUsize align(xbeam.utils.CACHE_LINE_LENGTH) = AtomicUsize.init(0),

        /// The buffer holding slots.
        buffer: [CAPACITY]Slot = comptime blk: {
            var result: [CAPACITY]Slot = undefined;

            var i: usize = 0;
            while (i < CAPACITY) : (i += 1) {
                result[i] = Slot{ .stamp = AtomicUsize.init(i) };
            }

            break :blk result;
        },

        pub fn init() Self {
            return .{};
        }

        /// Attempts to push an element into the queue.
        /// If the queue is full, returns an error.
        pub fn push(self: *Self, value: T) !void {
            var backoff = xbeam.utils.Backoff.init();
            var tail = self.tail.load(.Unordered);

            while (true) {
                // deconstruct the tail
                const index = tail & (one_lap - 1);
                const lap = tail & ~(one_lap - 1);

                // inspect the corresponding slot.
                const slot: *Slot = &self.buffer[index];
                const stamp = slot.stamp.load(.Acquire);

                // if the tail and the stamp match, we may attempt to push.
                if (tail == stamp) {
                    const new_tail = blk: {
                        if (index + 1 < CAPACITY) {
                            // Same lap, incremented index.
                            break :blk tail + 1;
                        } else {
                            // One lap forward, index wraps around to zero.
                            break :blk lap +% one_lap;
                        }
                    };

                    // try moving the tail.
                    if (@cmpxchgWeak(usize, &self.tail.unprotected_value, tail, new_tail, .SeqCst, .Monotonic)) |t| {
                        // failed to swap
                        tail = t;
                        backoff.spin();
                    } else {
                        slot.value = value;
                        slot.stamp.store(tail + 1, .Release);
                        return;
                    }
                } else if (stamp +% one_lap == tail + 1) {
                    @fence(.SeqCst);
                    const head = self.head.load(.Unordered);

                    // if the head lags one lap behind the tail as well...
                    if (head +% one_lap == tail) {
                        // ...then the queue is full
                        return error.QueueIsFull;
                    }

                    backoff.spin();
                    tail = self.tail.load(.Unordered);
                } else {
                    // we need to wait for the stamp to get updated.
                    backoff.snooze();
                    tail = self.tail.load(.Unordered);
                }
            }
        }

        /// Attempts to pop an element from the queue.
        /// If the queue is empty, `null` is returned.
        pub fn pop(self: *Self) ?T {
            var backoff = xbeam.utils.Backoff.init();
            var head = self.head.load(.Unordered);

            while (true) {
                // deconstruct the head
                const index = head & (one_lap - 1);
                const lap = head & ~(one_lap - 1);

                // inspect the corresponding slot.
                const slot: *Slot = &self.buffer[index];
                const stamp = slot.stamp.load(.Acquire);

                // if the the stamp is ahead of the head by 1, we may attempt to pop.
                if (head + 1 == stamp) {
                    const new_head = blk: {
                        if (index + 1 < CAPACITY) {
                            // Same lap, incremented index.
                            break :blk head + 1;
                        } else {
                            // One lap forward, index wraps around to zero.
                            break :blk lap +% one_lap;
                        }
                    };

                    // try moving the head.
                    if (@cmpxchgWeak(usize, &self.head.unprotected_value, head, new_head, .SeqCst, .Monotonic)) |h| {
                        // failed to swap
                        head = h;
                        backoff.spin();
                    } else {
                        const msg = slot.value;
                        slot.stamp.store(head +% one_lap, .Release);
                        return msg;
                    }
                } else if (stamp == head) {
                    @fence(.SeqCst);
                    const tail = self.head.load(.Unordered);

                    // If the tail equals the head, that means the queue is empty.
                    if (tail == head) return null;

                    backoff.spin();
                    head = self.head.load(.Unordered);
                } else {
                    // we need to wait for the stamp to get updated.
                    backoff.snooze();
                    head = self.head.load(.Unordered);
                }
            }
        }

        /// Returns `true` if the queue is empty.
        pub fn isEmpty(self: *Self) bool {
            const head = self.head.load(.SeqCst);
            const tail = self.tail.load(.SeqCst);

            // Is the tail lagging one lap behind head?
            // Is the tail equal to the head?
            //
            // Note: If the head changes just before we load the tail, that means there was a moment
            // when the channel was not empty, so it is safe to just return `false`.
            return head == tail;
        }

        /// Returns `true` if the queue is full.
        pub fn isFull(self: *Self) bool {
            const head = self.head.load(.SeqCst);
            const tail = self.tail.load(.SeqCst);

            // Is the head lagging one lap behind tail?
            //
            // Note: If the tail changes just before we load the head, that means there was a moment
            // when the queue was not full, so it is safe to just return `false`.
            return head +% one_lap == tail;
        }

        pub fn len(self: *Self) usize {
            while (true) {
                const head = self.head.load(.SeqCst);
                const tail = self.tail.load(.SeqCst);

                // If the tail didn't change, we've got consistent values to work with.
                if (self.tail.load(.SeqCst) == tail) {
                    const hix = head & (one_lap - 1);
                    const tix = tail & (one_lap - 1);

                    if (hix < tix) {
                        return tix - hix;
                    } else if (hix > tix) {
                        return CAPACITY - hix + tix;
                    } else if (tail == head) {
                        return 0;
                    } else {
                        return CAPACITY;
                    }
                }
            }
        }

        comptime {
            std.testing.refAllDecls(@This());
        }
    };
}

comptime {
    std.testing.refAllDecls(@This());
}

test "distinct buffers" {
    var q1 = ArrayQueueNoAlloc(usize, 10).init();
    var q2 = ArrayQueueNoAlloc(usize, 10).init();

    q1.buffer[0].value = 10;
    q2.buffer[0].value = 20;

    std.testing.expectEqual(@as(usize, 10), q1.buffer[0].value);
    std.testing.expectEqual(@as(usize, 20), q2.buffer[0].value);
}

test "push" {
    var q = ArrayQueueNoAlloc(usize, 2).init();

    try q.push(10);
    try q.push(20);

    std.testing.expectEqual(@as(usize, 10), q.buffer[0].value);
    std.testing.expectEqual(@as(usize, 20), q.buffer[1].value);

    std.testing.expectError(error.QueueIsFull, q.push(0));
}

test "isEmpty" {
    var q = ArrayQueueNoAlloc(usize, 2).init();

    std.testing.expectEqual(true, q.isEmpty());
    try q.push(10);
    std.testing.expectEqual(false, q.isEmpty());

    std.testing.expectEqual(@as(usize, 10), q.pop().?);
    std.testing.expectEqual(true, q.isEmpty());
}

test "isFull" {
    var q = ArrayQueueNoAlloc(usize, 2).init();

    std.testing.expectEqual(false, q.isFull());
    try q.push(10);
    std.testing.expectEqual(false, q.isFull());
    try q.push(20);
    std.testing.expectEqual(true, q.isFull());

    std.testing.expectEqual(@as(usize, 10), q.pop().?);

    std.testing.expectEqual(false, q.isFull());
}

test "len" {
    var q = ArrayQueueNoAlloc(usize, 2).init();

    std.testing.expectEqual(@as(usize, 0), q.len());

    try q.push(10);
    std.testing.expectEqual(@as(usize, 1), q.len());

    try q.push(20);
    std.testing.expectEqual(@as(usize, 2), q.len());

    std.testing.expectEqual(@as(usize, 10), q.pop().?);
    std.testing.expectEqual(@as(usize, 1), q.len());

    std.testing.expectEqual(@as(usize, 20), q.pop().?);
    std.testing.expectEqual(@as(usize, 0), q.len());
}

test "pop" {
    var q = ArrayQueueNoAlloc(usize, 2).init();

    try q.push(10);
    try q.push(20);
    std.testing.expectError(error.QueueIsFull, q.push(100));

    std.testing.expectEqual(@as(usize, 10), q.pop().?);
    std.testing.expectEqual(@as(usize, 20), q.pop().?);
    std.testing.expect(q.pop() == null);

    try q.push(0);
    try q.push(1);
    std.testing.expectError(error.QueueIsFull, q.push(100));

    std.testing.expectEqual(@as(usize, 0), q.pop().?);
    std.testing.expectEqual(@as(usize, 1), q.pop().?);
    std.testing.expect(q.pop() == null);
}
