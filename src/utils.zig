const std = @import("std");
const builtin = @import("builtin");

/// In concurrent programming, sometimes it is desirable to make sure commonly accessed pieces of
/// data are not placed into the same cache line. Updating an atomic value invalidates the whole
/// cache line it belongs to, which makes the next access to the same cache line slower for other
/// CPU cores.
/// Use `align(utils.CACHE_LINE_LENGTH)` to ensure updating one piece of data doesn't invalidate other cached data.
///
/// # Size and alignment
///
/// Cache lines are assumed to be N bytes long, depending on the architecture:
///
/// * On x86-64 and aarch64, N = 128.
/// * On all others, N = 64.
///
/// Note that N is just a reasonable guess and is not guaranteed to match the actual cache line
/// length of the machine the program is running on. On modern Intel architectures, spatial
/// prefetcher is pulling pairs of 64-byte cache lines at a time, so we pessimistically assume that
/// cache lines are 128 bytes long.
pub const CACHE_LINE_LENGTH: usize = switch (std.builtin.cpu.arch) {
    .x86_64, .aarch64 => 128,
    else => 64,
};

pub const Backoff = struct {
    const SPIN_LIMIT = 6;
    const YIELD_LIMIT = 10;

    step: u6 = 0,

    pub fn init() Backoff {
        return .{};
    }

    pub fn reset(self: *Backoff) void {
        self.step = 0;
    }

    pub fn spin(self: *Backoff) void {
        const step = self.step;

        const spins: usize = @as(usize, 1) << if (step < SPIN_LIMIT) blk: {
            self.step += 1;
            break :blk step;
        } else SPIN_LIMIT;

        // TODO: Pull request to break this out of `SpinLock`
        std.SpinLock.loopHint(spins);
    }

    pub fn snooze(self: *Backoff) void {
        const step = self.step;

        if (step <= SPIN_LIMIT) {
            std.SpinLock.loopHint(@as(usize, 1) << step);
        } else {
            yield();
        }

        if (step < YIELD_LIMIT) self.step += 1;
    }

    test "" {
        std.testing.refAllDecls(@This());
    }
};

// taken from `std.SpinLock`
// calling `std.SpinLock.yield()` directly is not possible if we want freestanding to be able to call this easily
fn yield() void {
    // On native windows, SwitchToThread is too expensive,
    // and yielding for 380-410 iterations was found to be
    // a nice sweet spot. Posix systems on the other hand,
    // especially linux, perform better by yielding the thread.
    switch (builtin.os.tag) {
        .windows => std.SpinLock.loopHint(400),
        .freestanding => {
            if (comptime @hasDecl(std.os, "sched_yield")) {
                std.os.sched_yield() catch std.SpinLock.loopHint(1);
            } else {
                std.SpinLock.loopHint(400);
            }
        },
        else => std.os.sched_yield() catch std.SpinLock.loopHint(1),
    }
}

test "" {
    std.testing.refAllDecls(@This());
}
