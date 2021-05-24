//! The implementation is based on Dmitry Vyukov's bounded MPMC queue.
//!
//! Source:
//!   - <http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue>

const xbeam = @import("../index.zig");
const std = @import("std");

const AtomicUsize = std.atomic.Int(usize);

/// A bounded multi-producer multi-consumer queue.
///
/// This queue allocates a fixed-capacity buffer on construction, which is used to store pushed
/// elements. The queue cannot hold more elements than the buffer allows. Attempting to push an
/// element into a full queue will fail.
pub fn ArrayQueue(comptime T: type) type {
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

        allocator: *std.mem.Allocator,

        /// A stamp with the value of `{ lap: 1, index: 0 }`.
        one_lap: usize,

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
        buffer: []Slot,

        pub fn init(allocator: *std.mem.Allocator, capacity: usize) !Self {
            const buffer = try allocator.alloc(Slot, capacity);
            for (buffer) |*slot, i| {
                slot.* = Slot{ .stamp = AtomicUsize.init(i) };
            }

            return Self{
                .allocator = allocator,
                .buffer = buffer,
                .one_lap = try std.math.ceilPowerOfTwo(usize, capacity + 1),
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.buffer);
        }

        /// Attempts to push an element into the queue.
        /// If the queue is full, returns an error.
        pub fn push(self: *Self, value: T) !void {
            const one_lap = self.one_lap;

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
                        if (index + 1 < self.buffer.len) {
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
            const one_lap = self.one_lap;

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
                        if (index + 1 < self.buffer.len) {
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
            return head +% self.one_lap == tail;
        }

        pub fn len(self: *Self) usize {
            const one_lap = self.one_lap;
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
                        return self.buffer.len - hix + tix;
                    } else if (tail == head) {
                        return 0;
                    } else {
                        return self.buffer.len;
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
    var q1 = try ArrayQueue(usize).init(std.testing.allocator, 10);
    defer q1.deinit();
    var q2 = try ArrayQueue(usize).init(std.testing.allocator, 10);
    defer q2.deinit();

    q1.buffer[0].value = 10;
    q2.buffer[0].value = 20;

    try std.testing.expectEqual(@as(usize, 10), q1.buffer[0].value);
    try std.testing.expectEqual(@as(usize, 20), q2.buffer[0].value);
}

test "push" {
    var q = try ArrayQueue(usize).init(std.testing.allocator, 2);
    defer q.deinit();

    try q.push(10);
    try q.push(20);

    try std.testing.expectEqual(@as(usize, 10), q.buffer[0].value);
    try std.testing.expectEqual(@as(usize, 20), q.buffer[1].value);

    try std.testing.expectError(error.QueueIsFull, q.push(0));
}

test "isEmpty" {
    var q = try ArrayQueue(usize).init(std.testing.allocator, 2);
    defer q.deinit();

    try std.testing.expectEqual(true, q.isEmpty());
    try q.push(10);
    try std.testing.expectEqual(false, q.isEmpty());

    try std.testing.expectEqual(@as(usize, 10), q.pop().?);
    try std.testing.expectEqual(true, q.isEmpty());
}

test "isFull" {
    var q = try ArrayQueue(usize).init(std.testing.allocator, 2);
    defer q.deinit();

    try std.testing.expectEqual(false, q.isFull());
    try q.push(10);
    try std.testing.expectEqual(false, q.isFull());
    try q.push(20);
    try std.testing.expectEqual(true, q.isFull());

    try std.testing.expectEqual(@as(usize, 10), q.pop().?);

    try std.testing.expectEqual(false, q.isFull());
}

test "len" {
    var q = try ArrayQueue(usize).init(std.testing.allocator, 2);
    defer q.deinit();

    try std.testing.expectEqual(@as(usize, 0), q.len());

    try q.push(10);
    try std.testing.expectEqual(@as(usize, 1), q.len());

    try q.push(20);
    try std.testing.expectEqual(@as(usize, 2), q.len());

    try std.testing.expectEqual(@as(usize, 10), q.pop().?);
    try std.testing.expectEqual(@as(usize, 1), q.len());

    try std.testing.expectEqual(@as(usize, 20), q.pop().?);
    try std.testing.expectEqual(@as(usize, 0), q.len());
}

test "pop" {
    var q = try ArrayQueue(usize).init(std.testing.allocator, 2);
    defer q.deinit();

    try q.push(10);
    try q.push(20);
    try std.testing.expectError(error.QueueIsFull, q.push(100));

    try std.testing.expectEqual(@as(usize, 10), q.pop().?);
    try std.testing.expectEqual(@as(usize, 20), q.pop().?);
    try std.testing.expect(q.pop() == null);

    try q.push(0);
    try q.push(1);
    try std.testing.expectError(error.QueueIsFull, q.push(100));

    try std.testing.expectEqual(@as(usize, 0), q.pop().?);
    try std.testing.expectEqual(@as(usize, 1), q.pop().?);
    try std.testing.expect(q.pop() == null);
}
