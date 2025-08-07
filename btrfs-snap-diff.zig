// =============================================================================
// BTRFS Snapshot Diff
// =============================================================================
// A fast BTRFS snapshot comparison tool for file and package change detection
// Author:  nzk0 (https://github.com/nzk0)
// Version: 1.0.0
// License: MIT
// Repo:    https://github.com/nzk0/btrfs-snap-diff
// =============================================================================

const std = @import("std");
const print = std.debug.print;
const fs = std.fs;
const mem = std.mem;
const process = std.process;
const time = std.time;
const Thread = std.Thread;
const Allocator = mem.Allocator;

// Terminal color codes
const Color = struct {
    const reset = "\x1b[0m";
    const bold = "\x1b[1m";
    const red = "\x1b[31m";
    const green = "\x1b[32m";
    const yellow = "\x1b[33m";
    const blue = "\x1b[34m";
    const cyan = "\x1b[36m";
    const gray = "\x1b[90m";
    const bold_red = "\x1b[1;31m";
    const bold_green = "\x1b[1;32m";
    const bold_yellow = "\x1b[1;33m";
    const bold_blue = "\x1b[1;34m";
    const bold_cyan = "\x1b[1;36m";
};

// Configuration
const Config = struct {
    use_color: bool = true,
    compact_mode: bool = false,
    tree_view: bool = true,
    
    fn init() Config {
        // Check if stdout is a TTY for color support
        const stdout = std.io.getStdOut();
        const use_color = std.posix.isatty(stdout.handle);
        
        // Check for NO_COLOR env var
        if (std.process.getEnvVarOwned(std.heap.page_allocator, "NO_COLOR") catch null) |v| {
            defer std.heap.page_allocator.free(v);
            return Config{ .use_color = false };
        }
        
        return Config{ .use_color = use_color };
    }
};

const Snapshot = struct {
    path: []const u8,
    num: u32,
    timestamp: i64,
    size_bytes: u64 = 0,
    file_count: u32 = 0,
    snapshot_type: SnapshotType = .unknown,
    description: ?[]const u8 = null,
    
    const SnapshotType = enum {
        unknown,
        manual,
        automatic,
        pre_update,
        post_update,
        scheduled,
    };
    
    fn lessThan(_: void, a: Snapshot, b: Snapshot) bool {
        return a.num < b.num;
    }
    
    fn getTypeString(self: Snapshot) []const u8 {
        return switch (self.snapshot_type) {
            .manual => "manual",
            .automatic => "auto",
            .pre_update => "pre-update",
            .post_update => "post-update",
            .scheduled => "scheduled",
            .unknown => "unknown",
        };
    }
    
    fn getTypeColor(self: Snapshot) []const u8 {
        return switch (self.snapshot_type) {
            .manual => Color.cyan,
            .automatic => Color.gray,
            .pre_update => Color.yellow,
            .post_update => Color.green,
            .scheduled => Color.blue,
            .unknown => "",
        };
    }
};

const FileChange = struct {
    path: []u8,
    change_type: ChangeType,
    size: u64 = 0,
    
    const ChangeType = enum {
        added,
        deleted,
        modified,
    };
};

const DiffResult = struct {
    added: std.ArrayList(FileChange),
    deleted: std.ArrayList(FileChange),
    modified: std.ArrayList(FileChange),
    total_added_size: u64 = 0,
    total_deleted_size: u64 = 0,
    total_modified_size: u64 = 0,
    
    fn init(a: Allocator) DiffResult {
        return .{
            .added = std.ArrayList(FileChange).init(a),
            .deleted = std.ArrayList(FileChange).init(a),
            .modified = std.ArrayList(FileChange).init(a),
        };
    }
    
    fn deinit(self: *DiffResult, a: Allocator) void {
        for (self.added.items) |item| a.free(item.path);
        for (self.deleted.items) |item| a.free(item.path);
        for (self.modified.items) |item| a.free(item.path);
        self.added.deinit();
        self.deleted.deinit();
        self.modified.deinit();
    }
    
    fn getTotalChanges(self: *const DiffResult) usize {
        return self.added.items.len + self.deleted.items.len + self.modified.items.len;
    }
};

const SnapperResult = struct {
    times: ?std.AutoHashMap(u32, SnapshotInfo),
    allocator: Allocator,
};

const SnapshotInfo = struct {
    timestamp: i64,
    snapshot_type: Snapshot.SnapshotType,
    description: ?[]const u8,
};

const PackageAction = enum { added, removed, updated };

const SECONDS_PER_DAY: i64 = 86400;
const SECONDS_PER_WEEK: i64 = 604800;
const SECONDS_PER_HOUR: i64 = 3600;
const MAX_HOUR: u8 = 23;
const MAX_MINUTE: u8 = 59;

var config: Config = undefined;

pub fn main() !void {
    config = Config.init();
    
    // Auto-elevate to root
    if (std.os.linux.getuid() != 0) {
        try autoElevate();
    }
    
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const args = try process.argsAlloc(allocator);
    defer process.argsFree(allocator, args);
    
    // Parse --no-color flag
    for (args) |arg| {
        if (mem.eql(u8, arg, "--no-color")) {
            config.use_color = false;
        }
    }
    
    if (args.len < 2) return usage();
    
    // Debug: Show what we're doing
    if (config.use_color) {
        printInfo("Scanning for snapshots...\n", .{});
    }
    
    const start_time = time.milliTimestamp();
    
    const snaps = try findSnapshotsParallel(allocator);
    defer {
        for (snaps.items) |snap| {
            allocator.free(snap.path);
            if (snap.description) |desc| allocator.free(desc);
        }
        snaps.deinit();
    }
    
    const scan_time = time.milliTimestamp() - start_time;
    
    if (snaps.items.len == 0) {
        printError("No snapshots found. Make sure BTRFS snapshots exist in /.snapshots or /timeshift-btrfs/snapshots\n", .{});
        return;
    }
    
    if (mem.eql(u8, args[1], "list")) {
        var show_count: ?usize = null;
        if (args.len > 2) {
            if (mem.eql(u8, args[2], "--all")) {
                show_count = std.math.maxInt(usize);
            } else if (std.fmt.parseInt(usize, args[2], 10) catch null) |n| {
                show_count = n;
            }
        }
        try cmdList(snaps.items, show_count, scan_time);
    } else if (mem.eql(u8, args[1], "diff")) {
        if (args.len < 4) {
            printError("diff command needs two snapshot references\n", .{});
            return usage();
        }
        try cmdDiff(allocator, snaps.items, args[2], args[3]);
    } else if (mem.eql(u8, args[1], "latest") or mem.eql(u8, args[1], "auto")) {
        if (snaps.items.len < 2) {
            printError("Need at least 2 snapshots for comparison\n", .{});
            return;
        }
        const n = snaps.items.len;
        printInfo("Comparing latest two snapshots:\n", .{});
        try diffSnapshots(allocator, snaps.items[n - 2], snaps.items[n - 1]);
    } else if (parseTimeRef(args[1])) |_| {
        if (args.len < 3) {
            printError("Need two time references for comparison\n", .{});
            return usage();
        }
        try cmdDiff(allocator, snaps.items, args[1], args[2]);
    } else {
        usage();
    }
}

fn autoElevate() !void {
    var buf: [4096]u8 = undefined;
    const exe = try fs.selfExePath(&buf);
    const alloc = std.heap.page_allocator;
    const args = try process.argsAlloc(alloc);
    defer process.argsFree(alloc, args);
    
    var cmd = std.ArrayList([]const u8).init(alloc);
    defer cmd.deinit();
    try cmd.append("sudo");
    try cmd.append(exe);
    for (args[1..]) |arg| try cmd.append(arg);
    
    var child = process.Child.init(cmd.items, alloc);
    child.stdin_behavior = .Inherit;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;
    
    try child.spawn();
    const term = try child.wait();
    
    process.exit(switch (term) {
        .Exited => |code| code,
        else => 1,
    });
}

fn usage() void {
    printHeader("BTRFS Snapshot Diff", .{});
    print("Compare BTRFS snapshots for file and package changes\n\n", .{});
    
    printSection("Usage:", .{});
    print("  btrfs-snap-diff <ref1> <ref2>           Compare two snapshots\n", .{});
    print("  btrfs-snap-diff diff <ref1> <ref2>      Same as above (optional 'diff' keyword)\n", .{});
    print("  btrfs-snap-diff latest                  Compare the two most recent snapshots\n", .{});
    print("  btrfs-snap-diff list [N|--all]          List N snapshots (default: 15)\n", .{});
    print("\n", .{});
    
    printSection("Options:", .{});
    print("  --no-color                              Disable colored output\n", .{});
    print("\n", .{});
    
    print("The 'diff' keyword is optional - both forms work identically.\n", .{});
    print("The tool automatically elevates to root privileges when needed.\n\n", .{});
    
    printSection("Directories scanned:", .{});
    print("  /etc, /usr/bin, /usr/lib, /var/lib/pacman/local\n\n", .{});
    
    printSection("Time References:", .{});
    print("  Relative: today, yesterday, latest, previous, oldest\n", .{});
    print("  Days ago: 2d, 3d, 1w (days/weeks before today)\n", .{});
    print("  With time: today:14:30, yesterday:09:15\n", .{});
    print("  Snapshot numbers: #265, 265\n\n", .{});
    
    printSection("Examples:", .{});
    print("  btrfs-snap-diff today yesterday         Compare today with yesterday\n", .{});
    print("  btrfs-snap-diff 2d today                Compare 2 days ago with today\n", .{});
    print("  btrfs-snap-diff yesterday:14:00 latest  Compare yesterday at 14:00 with latest\n", .{});
    print("  btrfs-snap-diff #265 #270               Compare specific snapshot numbers\n", .{});
    print("  btrfs-snap-diff list 30                 Show last 30 snapshots\n", .{});
    print("  btrfs-snap-diff list --all              Show all snapshots\n\n", .{});
    
    print("Snapshots are auto-discovered from Snapper (/.snapshots) and Timeshift.\n", .{});
}

// Color output helpers
fn printColor(color: []const u8, comptime fmt: []const u8, args: anytype) void {
    if (config.use_color) print("{s}", .{color});
    print(fmt, args);
    if (config.use_color) print("{s}", .{Color.reset});
}

fn printHeader(comptime fmt: []const u8, args: anytype) void {
    printColor(Color.bold_cyan, fmt, args);
    print("\n", .{});
}

fn printSection(comptime fmt: []const u8, args: anytype) void {
    printColor(Color.bold, fmt, args);
    print("\n", .{});
}

fn printSuccess(comptime fmt: []const u8, args: anytype) void {
    printColor(Color.green, fmt, args);
}

fn printError(comptime fmt: []const u8, args: anytype) void {
    printColor(Color.bold_red, "Error: ", .{});
    printColor(Color.red, fmt, args);
}

fn printWarning(comptime fmt: []const u8, args: anytype) void {
    printColor(Color.bold_yellow, "Warning: ", .{});
    printColor(Color.yellow, fmt, args);
}

fn printInfo(comptime fmt: []const u8, args: anytype) void {
    printColor(Color.cyan, fmt, args);
}

fn parseTimeRef(ref: []const u8) ?enum {
    today,
    yesterday,
    latest,
    previous,
    oldest,
    days_ago,
    date,
    number,
} {
    if (mem.eql(u8, ref, "today")) return .today;
    if (mem.eql(u8, ref, "yesterday")) return .yesterday;
    if (mem.eql(u8, ref, "latest") or mem.eql(u8, ref, "newest")) return .latest;
    if (mem.eql(u8, ref, "previous") or mem.eql(u8, ref, "prev")) return .previous;
    if (mem.eql(u8, ref, "oldest") or mem.eql(u8, ref, "first")) return .oldest;
    
    if (ref.len > 1) {
        const last = ref[ref.len - 1];
        if (last == 'd' or last == 'w') {
            _ = std.fmt.parseInt(u32, ref[0 .. ref.len - 1], 10) catch return null;
            return .days_ago;
        }
    }
    
    if (ref[0] == '#' or std.fmt.parseInt(u32, ref, 10) catch null != null) return .number;
    if (mem.indexOfScalar(u8, ref, '-') != null or mem.indexOfScalar(u8, ref, '/') != null) return .date;
    
    return null;
}

fn findSnapshotsParallel(allocator: Allocator) !std.ArrayList(Snapshot) {
    var list = std.ArrayList(Snapshot).init(allocator);
    errdefer {
        for (list.items) |snap| {
            allocator.free(snap.path);
            if (snap.description) |desc| allocator.free(desc);
        }
        list.deinit();
    }
    
    var mutex = Thread.Mutex{};
    
    // Get snapper info in parallel
    var snapper_result = SnapperResult{ .times = null, .allocator = allocator };
    var snapper_thread = try Thread.spawn(.{}, getSnapperInfoThread, .{ allocator, &snapper_result });
    
    // Scan directories
    const paths = [_][]const u8{ "/.snapshots", "/timeshift-btrfs/snapshots" };
    var threads: [paths.len]?Thread = .{null} ** paths.len;
    
    for (paths, 0..) |path, i| {
        threads[i] = Thread.spawn(.{}, scanSnapshotDir, .{
            allocator, path, &list, &mutex,
        }) catch null;
    }
    
    // Wait for results
    snapper_thread.join();
    defer if (snapper_result.times) |*st| {
        var it = st.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.*.description) |desc| {
                allocator.free(desc);
            }
        }
        st.deinit();
    };
    
    for (threads) |thread| {
        if (thread) |t| t.join();
    }
    
    // Update with snapper info
    if (snapper_result.times) |st| {
        mutex.lock();
        defer mutex.unlock();
        
        for (list.items) |*snap| {
            if (st.get(snap.num)) |info| {
                snap.timestamp = info.timestamp;
                snap.snapshot_type = info.snapshot_type;
                if (info.description) |desc| {
                    snap.description = try allocator.dupe(u8, desc);
                }
            }
        }
    }
    
    // Calculate snapshot sizes and file counts (skip for performance)
    // This was likely causing the hang - commenting out for now
    // for (list.items) |*snap| {
    //     const info = getSnapshotInfo(snap.path) catch continue;
    //     snap.size_bytes = info.size;
    //     snap.file_count = info.file_count;
    // }
    
    std.sort.insertion(Snapshot, list.items, {}, Snapshot.lessThan);
    return list;
}

const SnapshotStats = struct {
    size: u64,
    file_count: u32,
};

fn getSnapshotInfo(path: []const u8) !SnapshotStats {
    // Fast estimation using du command
    const cmd = try std.fmt.allocPrint(std.heap.page_allocator, 
        "du -sb {s} 2>/dev/null | cut -f1", .{path});
    defer std.heap.page_allocator.free(cmd);
    
    const result = process.Child.run(.{
        .allocator = std.heap.page_allocator,
        .argv = &[_][]const u8{ "sh", "-c", cmd },
        .max_output_bytes = 1024,
    }) catch return SnapshotStats{ .size = 0, .file_count = 0 };
    
    defer std.heap.page_allocator.free(result.stdout);
    defer std.heap.page_allocator.free(result.stderr);
    
    const size_str = mem.trim(u8, result.stdout, " \t\n");
    const size = std.fmt.parseInt(u64, size_str, 10) catch 0;
    
    // Estimate file count (rough)
    const file_count = @as(u32, @intCast(@min(size / 4096, std.math.maxInt(u32))));
    
    return SnapshotStats{ .size = size, .file_count = file_count };
}

fn getSnapperInfoThread(allocator: Allocator, result: *SnapperResult) void {
    result.times = getSnapperInfo(allocator);
}

fn getSnapperInfo(allocator: Allocator) ?std.AutoHashMap(u32, SnapshotInfo) {
    var snapper_info = std.AutoHashMap(u32, SnapshotInfo).init(allocator);
    errdefer snapper_info.deinit();
    
    // Try simpler snapper list command first
    const res = process.Child.run(.{
        .allocator = allocator,
        .argv = &[_][]const u8{ "snapper", "list", "--no-headers" },
        .max_output_bytes = 1024 * 1024,
    }) catch {
        // If snapper fails, try alternative
        const res2 = process.Child.run(.{
            .allocator = allocator,
            .argv = &[_][]const u8{ "snapper", "-c", "root", "list" },
            .max_output_bytes = 1024 * 1024,
        }) catch return null;
        
        defer allocator.free(res2.stdout);
        defer allocator.free(res2.stderr);
        
        return parseSnapperOutput(allocator, res2.stdout, &snapper_info);
    };
    
    defer allocator.free(res.stdout);
    defer allocator.free(res.stderr);
    
    return parseSnapperOutput(allocator, res.stdout, &snapper_info);
}

fn parseSnapperOutput(allocator: Allocator, output: []const u8, snapper_info: *std.AutoHashMap(u32, SnapshotInfo)) ?std.AutoHashMap(u32, SnapshotInfo) {
    var lines = mem.tokenizeScalar(u8, output, '\n');
    while (lines.next()) |line| {
        const trimmed = mem.trim(u8, line, " \t");
        if (trimmed.len == 0) continue;
        if (trimmed[0] < '0' or trimmed[0] > '9') continue;
        
        // Simple parsing - just get the number and timestamp
        var parts = mem.tokenizeAny(u8, trimmed, " \t|│");
        const num_str = parts.next() orelse continue;
        const num = std.fmt.parseInt(u32, num_str, 10) catch continue;
        
        // Skip to find date-like string
        var timestamp: i64 = 0;
        while (parts.next()) |part| {
            // Look for year pattern (20xx)
            if (part.len >= 4 and mem.startsWith(u8, part, "20")) {
                // This might be a date, try to parse the whole remaining line as a date
                const date_start_idx = mem.indexOf(u8, trimmed, part) orelse continue;
                const date_str = trimmed[date_start_idx..];
                
                // Find the end of the date (usually before description)
                var date_end: usize = date_str.len;
                for (date_str, 0..) |c, i| {
                    if (i > 10 and (c == '|' or c == '\t' or c == '│')) {
                        date_end = i;
                        break;
                    }
                }
                
                const clean_date = mem.trim(u8, date_str[0..date_end], " \t");
                
                // Use date command to parse
                const cmd = std.fmt.allocPrint(allocator, 
                    "date -d '{s}' '+%s' 2>/dev/null", .{clean_date}) catch continue;
                defer allocator.free(cmd);
                
                const date_res = process.Child.run(.{
                    .allocator = allocator,
                    .argv = &[_][]const u8{ "sh", "-c", cmd },
                    .max_output_bytes = 1024,
                }) catch continue;
                
                defer allocator.free(date_res.stdout);
                defer allocator.free(date_res.stderr);
                
                if (date_res.term.Exited == 0) {
                    const ts_str = mem.trim(u8, date_res.stdout, " \t\n");
                    timestamp = std.fmt.parseInt(i64, ts_str, 10) catch 0;
                    break;
                }
            }
        }
        
        if (timestamp > 0) {
            snapper_info.put(num, .{
                .timestamp = timestamp,
                .snapshot_type = .automatic,
                .description = null,
            }) catch continue;
        }
    }
    
    return if (snapper_info.count() > 0) snapper_info.* else null;
}

fn scanSnapshotDir(
    allocator: Allocator,
    base: []const u8,
    list: *std.ArrayList(Snapshot),
    mutex: *Thread.Mutex,
) void {
    const dir = fs.openDirAbsolute(base, .{ .iterate = true }) catch return;
    var it = dir.iterate();
    
    var local_snaps = std.ArrayList(Snapshot).init(allocator);
    defer local_snaps.deinit();
    
    while (it.next() catch null) |e| {
        if (e.kind != .directory) continue;
        
        const snap_path = std.fmt.allocPrint(allocator, 
            "{s}/{s}/snapshot", .{ base, e.name }) catch continue;
        errdefer allocator.free(snap_path);
        
        fs.accessAbsolute(snap_path, .{}) catch {
            allocator.free(snap_path);
            continue;
        };
        
        // Extract number
        var num: u32 = 0;
        var found_digit = false;
        for (e.name) |c| {
            if (c >= '0' and c <= '9') {
                found_digit = true;
                const digit = @as(u32, c - '0');
                if (num > (std.math.maxInt(u32) - digit) / 10) break;
                num = num * 10 + digit;
            }
        }
        
        if (!found_digit) {
            allocator.free(snap_path);
            continue;
        }
        
        const stat = dir.statFile(e.name) catch {
            allocator.free(snap_path);
            continue;
        };
        const timestamp = @as(i64, @intCast(@divTrunc(stat.mtime, 1_000_000_000)));
        
        local_snaps.append(.{
            .path = snap_path,
            .num = num,
            .timestamp = timestamp,
        }) catch {
            allocator.free(snap_path);
            continue;
        };
    }
    
    if (local_snaps.items.len > 0) {
        mutex.lock();
        defer mutex.unlock();
        list.appendSlice(local_snaps.items) catch {};
    }
}

fn cmdList(snaps: []const Snapshot, show_count: ?usize, scan_time: i64) !void {
    if (snaps.len == 0) {
        printWarning("No snapshots found\n", .{});
        return;
    }
    
    printHeader("Found {} snapshots", .{snaps.len});
    printColor(Color.gray, " (scanned in {}ms)\n\n", .{scan_time});
    
    const default_show = 15;
    const show_all = show_count != null and show_count.? == std.math.maxInt(usize);
    const show_n = if (show_all) snaps.len else (show_count orelse @min(default_show, snaps.len));
    const actual_show = @min(show_n, snaps.len);
    const start = if (actual_show >= snaps.len) 0 else snaps.len - actual_show;
    
    if (!show_all and snaps.len > default_show and show_count == null) {
        printInfo("Showing last {} snapshots (use 'list --all' or 'list {}' for all):\n\n", 
            .{ actual_show, snaps.len });
    }
    
    const now = time.timestamp();
    
    // Show in reverse (newest first)
    var display_num: usize = actual_show;
    var i = snaps.len;
    while (i > start) : (i -= 1) {
        const snap = snaps[i - 1];
        
        // Snapshot number with color
        printColor(Color.bold, "  {:2}. ", .{display_num});
        printColor(Color.cyan, "#{}", .{snap.num});
        
        // Type and indicator
        if (snap.snapshot_type != .unknown) {
            print(" ", .{});
            printColor(snap.getTypeColor(), "[{s}]", .{snap.getTypeString()});
        }
        
        // Has changes indicator (● or ○)
        const has_changes = i > 1 and checkHasChanges(snaps[i - 2].path, snap.path);
        if (has_changes) {
            printColor(Color.green, " ●", .{});
        } else {
            printColor(Color.gray, " ○", .{});
        }
        
        // Timestamp
        const age_seconds = now - snap.timestamp;
        const age_days = @divTrunc(age_seconds, SECONDS_PER_DAY);
        const age_hours = @divTrunc(@mod(age_seconds, SECONDS_PER_DAY), SECONDS_PER_HOUR);
        
        print(" ", .{});
        if (getFormattedTime(snap.timestamp)) |time_str| {
            defer std.heap.page_allocator.free(time_str);
            print("{s}", .{time_str});
        }
        
        // Relative age
        printColor(Color.gray, " (", .{});
        if (age_days > 0) {
            printColor(Color.gray, "{} day{s} ago", 
                .{ age_days, if (age_days == 1) "" else "s" });
        } else if (age_hours > 0) {
            printColor(Color.gray, "{} hour{s} ago", 
                .{ age_hours, if (age_hours == 1) "" else "s" });
        } else {
            printColor(Color.gray, "recent", .{});
        }
        printColor(Color.gray, ")", .{});
        
        // Size info
        if (snap.size_bytes > 0) {
            print(" ", .{});
            printColor(Color.gray, "[{s}]", .{formatBytes(snap.size_bytes)});
        }
        
        // Tags
        if (i == snaps.len) {
            printColor(Color.bold_green, " ← latest", .{});
        }
        if (i == snaps.len - 1 and snaps.len > 1) {
            printColor(Color.bold_yellow, " ← previous", .{});
        }
        
        // Description on new line if present
        if (snap.description) |desc| {
            print("\n      ", .{});
            printColor(Color.gray, "\"{s}\"", .{desc});
        }
        
        print("\n", .{});
        display_num -= 1;
    }
}

fn checkHasChanges(path1: []const u8, path2: []const u8) bool {
    // Quick check if snapshots differ - disabled for performance
    // This was causing hangs on large snapshots
    _ = path1;
    _ = path2;
    return false;
}

fn formatBytes(bytes: u64) []const u8 {
    const units = [_][]const u8{ "B", "KB", "MB", "GB", "TB" };
    var size = @as(f64, @floatFromInt(bytes));
    var unit_idx: usize = 0;
    
    while (size >= 1024 and unit_idx < units.len - 1) : (unit_idx += 1) {
        size /= 1024;
    }
    
    var buf: [32]u8 = undefined;
    const result = std.fmt.bufPrint(&buf, "{d:.1} {s}", .{ size, units[unit_idx] }) catch "? B";
    return std.heap.page_allocator.dupe(u8, result) catch "? B";
}

fn getFormattedTime(timestamp: i64) ?[]u8 {
    const cmd = std.fmt.allocPrint(std.heap.page_allocator,
        "date -d @{} '+%Y-%m-%d %H:%M'", .{timestamp}) catch return null;
    defer std.heap.page_allocator.free(cmd);
    
    const res = process.Child.run(.{
        .allocator = std.heap.page_allocator,
        .argv = &[_][]const u8{ "sh", "-c", cmd },
        .max_output_bytes = 1024,
    }) catch return null;
    
    defer std.heap.page_allocator.free(res.stdout);
    defer std.heap.page_allocator.free(res.stderr);
    
    const time_str = mem.trim(u8, res.stdout, " \t\n");
    return std.heap.page_allocator.dupe(u8, time_str) catch null;
}

fn readUserChoice(max: usize) !usize {
    const stdin = std.io.getStdIn().reader();
    var buf: [10]u8 = undefined;
    
    while (true) {
        printInfo("Select [1-{}] (Enter = 1 for most recent): ", .{max});
        if (try stdin.readUntilDelimiterOrEof(&buf, '\n')) |line| {
            const trimmed = mem.trim(u8, line, " \r\n");
            if (trimmed.len == 0) {
                printSuccess("Using most recent (option 1).\n", .{});
                return 1;
            }
            const choice = std.fmt.parseInt(usize, trimmed, 10) catch {
                printError("Invalid input. Please enter a number.\n", .{});
                continue;
            };
            if (choice >= 1 and choice <= max) return choice;
            printError("Please enter a number between 1 and {}.\n", .{max});
        }
    }
}

fn selectSnapshot(matches: []const *const Snapshot, ref: []const u8, show_closest: bool) !*const Snapshot {
    if (matches.len == 1) return matches[0];
    
    if (matches.len == 0) {
        printError("No snapshots found for '{s}'\n", .{ref});
        printInfo("Hint: Use 'list' to see available snapshots\n", .{});
        return error.NoSnapshotFound;
    }
    
    const now = time.timestamp();
    
    if (show_closest) {
        printWarning("No exact match for '{s}'. Showing closest matches:\n", .{ref});
    } else {
        printInfo("Multiple snapshots found for '{s}':\n", .{ref});
    }
    
    // Show newest first
    var display_num: usize = 1;
    var i = matches.len;
    while (i > 0) : (i -= 1) {
        const s = matches[i - 1];
        
        printColor(Color.bold, "  {}. ", .{display_num});
        printColor(Color.cyan, "#{}", .{s.num});
        
        if (s.snapshot_type != .unknown) {
            print(" ", .{});
            printColor(s.getTypeColor(), "[{s}]", .{s.getTypeString()});
        }
        
        print(" ", .{});
        if (getFormattedTime(s.timestamp)) |time_str| {
            defer std.heap.page_allocator.free(time_str);
            print("{s}", .{time_str});
        }
        
        // Relative age
        const age_seconds = now - s.timestamp;
        const age_days = @divTrunc(age_seconds, SECONDS_PER_DAY);
        const age_hours = @divTrunc(@mod(age_seconds, SECONDS_PER_DAY), SECONDS_PER_HOUR);
        
        printColor(Color.gray, " (", .{});
        if (age_days > 0) {
            printColor(Color.gray, "{} day{s} ago", 
                .{ age_days, if (age_days == 1) "" else "s" });
        } else if (age_hours > 0) {
            printColor(Color.gray, "{} hour{s} ago", 
                .{ age_hours, if (age_hours == 1) "" else "s" });
        } else {
            printColor(Color.gray, "recent", .{});
        }
        printColor(Color.gray, ")", .{});
        
        if (i == matches.len) {
            if (show_closest) {
                printColor(Color.yellow, " ← closest match", .{});
            } else {
                printColor(Color.green, " ← most recent", .{});
            }
        }
        
        print("\n", .{});
        display_num += 1;
    }
    
    const choice = try readUserChoice(matches.len);
    return matches[matches.len - choice];
}

const TimeSpec = struct {
    base: []const u8,
    hour: ?u8,
    minute: ?u8,
};

fn parseTimeSpec(ref: []const u8) ?TimeSpec {
    if (mem.indexOfScalar(u8, ref, ':')) |colon| {
        const base = ref[0..colon];
        const time_part = ref[colon + 1 ..];
        
        if (mem.indexOfScalar(u8, time_part, ':')) |second_colon| {
            const hour = std.fmt.parseInt(u8, time_part[0..second_colon], 10) catch return null;
            const minute = std.fmt.parseInt(u8, time_part[second_colon + 1 ..], 10) catch return null;
            if (hour > MAX_HOUR or minute > MAX_MINUTE) return null;
            return TimeSpec{ .base = base, .hour = hour, .minute = minute };
        }
        
        if (time_part.len <= 2) {
            const hour = std.fmt.parseInt(u8, time_part, 10) catch return null;
            if (hour > MAX_HOUR) return null;
            return TimeSpec{ .base = base, .hour = hour, .minute = 0 };
        }
    }
    
    return TimeSpec{ .base = ref, .hour = null, .minute = null };
}

fn resolveSnapshot(snaps: []const Snapshot, ref: []const u8) !?*const Snapshot {
    if (snaps.len == 0) {
        printError("No snapshots available\n", .{});
        return null;
    }
    
    const spec = parseTimeSpec(ref);
    const base_ref = if (spec) |s| s.base else ref;
    
    // Special keywords
    if (mem.eql(u8, base_ref, "latest")) return &snaps[snaps.len - 1];
    if (mem.eql(u8, base_ref, "previous")) {
        if (snaps.len > 1) return &snaps[snaps.len - 2];
        printError("No previous snapshot available\n", .{});
        return null;
    }
    if (mem.eql(u8, base_ref, "oldest")) return &snaps[0];
    
    // Snapshot numbers
    if (base_ref[0] == '#') {
        const num = try std.fmt.parseInt(u32, base_ref[1..], 10);
        for (snaps) |*s| {
            if (s.num == num) return s;
        }
        // Find closest
        var closest: ?*const Snapshot = null;
        var min_diff: u32 = std.math.maxInt(u32);
        for (snaps) |*s| {
            const diff = if (s.num > num) s.num - num else num - s.num;
            if (diff < min_diff) {
                min_diff = diff;
                closest = s;
            }
        }
        if (closest) |c| {
            printWarning("Snapshot #{} not found. Did you mean #{}? (closest match)\n", 
                .{ num, c.num });
            const confirm = try askConfirmation();
            if (confirm) return c;
        }
        return null;
    } else if (std.fmt.parseInt(u32, base_ref, 10) catch null) |num| {
        for (snaps) |*s| {
            if (s.num == num) return s;
        }
        printError("Snapshot #{} not found\n", .{num});
        return null;
    }
    
    // Time-based resolution
    const now = time.timestamp();
    const tz_offset = getTimezoneOffset();
    const local_now = now + tz_offset;
    
    var target_time: i64 = local_now;
    var is_relative_day = false;
    
    if (mem.eql(u8, base_ref, "today")) {
        target_time = local_now;
        is_relative_day = true;
    } else if (mem.eql(u8, base_ref, "yesterday")) {
        target_time = local_now - SECONDS_PER_DAY;
        is_relative_day = true;
    } else if (base_ref.len > 1 and base_ref[base_ref.len - 1] == 'd') {
        const days = try std.fmt.parseInt(u32, base_ref[0 .. base_ref.len - 1], 10);
        if (days == 0) {
            printError("Invalid time reference: 0 days ago is 'today'\n", .{});
            return null;
        }
        target_time = local_now - (days * SECONDS_PER_DAY);
        is_relative_day = true;
    } else if (base_ref.len > 1 and base_ref[base_ref.len - 1] == 'w') {
        const weeks = try std.fmt.parseInt(u32, base_ref[0 .. base_ref.len - 1], 10);
        target_time = local_now - (weeks * SECONDS_PER_WEEK);
        is_relative_day = true;
    } else {
        printError("Unknown time reference: '{s}'\n", .{ref});
        printInfo("Use 'list' to see available snapshots\n", .{});
        return null;
    }
    
    // Find matching snapshots
    const day_start = @divTrunc(target_time, SECONDS_PER_DAY) * SECONDS_PER_DAY;
    const day_end = day_start + SECONDS_PER_DAY;
    
    var matches = std.ArrayList(*const Snapshot).init(std.heap.page_allocator);
    defer matches.deinit();
    
    for (snaps) |*s| {
        const local_snap_time = s.timestamp + tz_offset;
        if (local_snap_time >= day_start and local_snap_time < day_end) {
            try matches.append(s);
        }
    }
    
    if (matches.items.len > 0) {
        if (spec) |sp| if (sp.hour) |target_hour| {
            const target_minutes = target_hour * 60 + (sp.minute orelse 0);
            var best: *const Snapshot = matches.items[0];
            var best_diff: u64 = std.math.maxInt(u64);
            
            for (matches.items) |s| {
                const local_snap_time = s.timestamp + tz_offset;
                const snap_seconds = @mod(local_snap_time, SECONDS_PER_DAY);
                const snap_minutes = @divTrunc(snap_seconds, 60);
                const time_diff = @abs(snap_minutes - target_minutes);
                if (time_diff < best_diff) {
                    best = s;
                    best_diff = time_diff;
                }
            }
            printSuccess("Found snapshot at specified time for '{s}': #{}\n", .{ ref, best.num });
            return best;
        };
        
        return try selectSnapshot(matches.items, ref, false);
    }
    
    // No exact match - find closest
    if (is_relative_day) {
        const max_closest = 3;
        var closest = std.ArrayList(*const Snapshot).init(std.heap.page_allocator);
        defer closest.deinit();
        
        const SnapDist = struct {
            snap: *const Snapshot,
            dist: u64,
            
            fn lessThan(_: void, a: @This(), b: @This()) bool {
                return a.dist < b.dist;
            }
        };
        
        var distances = std.ArrayList(SnapDist).init(std.heap.page_allocator);
        defer distances.deinit();
        
        for (snaps) |*s| {
            const dist = @abs(s.timestamp - (target_time - tz_offset));
            try distances.append(.{ .snap = s, .dist = dist });
        }
        
        std.sort.insertion(SnapDist, distances.items, {}, SnapDist.lessThan);
        
        const show_count = @min(max_closest, distances.items.len);
        for (distances.items[0..show_count]) |d| {
            try closest.append(d.snap);
        }
        
        if (closest.items.len > 0) {
            return try selectSnapshot(closest.items, ref, true);
        }
    }
    
    return null;
}

fn getTimezoneOffset() i64 {
    const res = process.Child.run(.{
        .allocator = std.heap.page_allocator,
        .argv = &[_][]const u8{ "date", "+%z" },
        .max_output_bytes = 32,
    }) catch return 0;
    
    defer std.heap.page_allocator.free(res.stdout);
    defer std.heap.page_allocator.free(res.stderr);
    
    const offset_str = mem.trim(u8, res.stdout, " \t\n");
    if (offset_str.len >= 3) {
        const hours = std.fmt.parseInt(i32, offset_str[0..3], 10) catch return 0;
        const minutes = if (offset_str.len >= 5) 
            std.fmt.parseInt(i32, offset_str[3..5], 10) catch 0 
        else 0;
        return hours * 3600 + (if (hours < 0) -minutes else minutes) * 60;
    }
    return 0;
}

fn askConfirmation() !bool {
    const stdin = std.io.getStdIn().reader();
    var buf: [10]u8 = undefined;
    
    printInfo("Use this snapshot? [y/N]: ", .{});
    if (try stdin.readUntilDelimiterOrEof(&buf, '\n')) |line| {
        const trimmed = mem.trim(u8, line, " \r\n");
        return mem.eql(u8, trimmed, "y") or mem.eql(u8, trimmed, "Y");
    }
    return false;
}

fn cmdDiff(allocator: Allocator, snaps: []const Snapshot, ref1: []const u8, ref2: []const u8) !void {
    const snap1 = try resolveSnapshot(snaps, ref1) orelse {
        printError("Cannot resolve '{s}'\n", .{ref1});
        return;
    };
    
    const snap2 = try resolveSnapshot(snaps, ref2) orelse {
        printError("Cannot resolve '{s}'\n", .{ref2});
        return;
    };
    
    if (snap1.num == snap2.num) {
        printWarning("Both references point to the same snapshot (#{})!\n", .{snap1.num});
        print("No differences to show.\n", .{});
        return;
    }
    
    try diffSnapshots(allocator, snap1.*, snap2.*);
}

fn diffSnapshots(allocator: Allocator, snap1: Snapshot, snap2: Snapshot) !void {
    // Find distance between snapshots
    printHeader("=== Snapshot Comparison ===", .{});
    
    printInfo("  From: ", .{});
    printColor(Color.cyan, "#{}", .{snap1.num});
    if (snap1.snapshot_type != .unknown) {
        print(" ", .{});
        printColor(snap1.getTypeColor(), "[{s}]", .{snap1.getTypeString()});
    }
    if (getFormattedTime(snap1.timestamp)) |time_str| {
        defer std.heap.page_allocator.free(time_str);
        print(" at {s}", .{time_str});
    }
    print("\n", .{});
    
    printInfo("  To:   ", .{});
    printColor(Color.cyan, "#{}", .{snap2.num});
    if (snap2.snapshot_type != .unknown) {
        print(" ", .{});
        printColor(snap2.getTypeColor(), "[{s}]", .{snap2.getTypeString()});
    }
    if (getFormattedTime(snap2.timestamp)) |time_str| {
        defer std.heap.page_allocator.free(time_str);
        print(" at {s}", .{time_str});
    }
    print("\n\n", .{});
    
    const start_time = time.milliTimestamp();
    
    var result = DiffResult.init(allocator);
    defer result.deinit(allocator);
    
    var mutex = Thread.Mutex{};
    
    const dirs = [_][]const u8{ "/etc", "/usr/bin", "/usr/lib", "/var/lib/pacman/local" };
    var threads: [dirs.len]?Thread = .{null} ** dirs.len;
    
    // Launch workers
    for (dirs, 0..) |rel, i| {
        threads[i] = Thread.spawn(.{}, compareDirWorker, .{
            allocator, snap1.path, snap2.path, rel, &result, &mutex,
        }) catch null;
    }
    
    // Wait for completion
    for (threads) |thread| {
        if (thread) |t| t.join();
    }
    
    const diff_time = time.milliTimestamp() - start_time;
    
    const total = result.getTotalChanges();
    if (total == 0) {
        printSuccess("No changes detected.\n", .{});
        return;
    }
    
    // Show results
    if (config.tree_view) {
        try showTreeView(allocator, &result);
    } else {
        try showFlatView(&result);
    }
    
    // Package changes
    try analyzePackageChanges(allocator, &result);
    
    // Summary
    printSection("\n=== Summary ===", .{});
    print("  Total: ", .{});
    printColor(Color.bold, "{} changes", .{total});
    printColor(Color.gray, " (analyzed in {}ms)\n", .{diff_time});
    
    if (result.added.items.len > 0) {
        print("  Added: {} files ", .{result.added.items.len});
        printColor(Color.green, "(+{s})", .{formatBytes(result.total_added_size)});
        print("\n", .{});
    }
    
    if (result.deleted.items.len > 0) {
        print("  Deleted: {} files ", .{result.deleted.items.len});
        printColor(Color.red, "(-{s})", .{formatBytes(result.total_deleted_size)});
        print("\n", .{});
    }
    
    if (result.modified.items.len > 0) {
        print("  Modified: {} files ", .{result.modified.items.len});
        printColor(Color.yellow, "(~{s})", .{formatBytes(result.total_modified_size)});
        print("\n", .{});
    }
    
    // Most changed directory
    var dir_changes = std.StringHashMap(u32).init(allocator);
    defer dir_changes.deinit();
    
    for (result.added.items) |fc| countDirChange(&dir_changes, fc.path);
    for (result.deleted.items) |fc| countDirChange(&dir_changes, fc.path);
    for (result.modified.items) |fc| countDirChange(&dir_changes, fc.path);
    
    var max_dir: ?[]const u8 = null;
    var max_count: u32 = 0;
    var it = dir_changes.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* > max_count) {
            max_count = entry.value_ptr.*;
            max_dir = entry.key_ptr.*;
        }
    }
    
    if (max_dir) |dir| {
        print("  Most changed: {s} ({} changes)\n", .{ dir, max_count });
    }
}

fn countDirChange(map: *std.StringHashMap(u32), path: []u8) void {
    if (mem.lastIndexOfScalar(u8, path, '/')) |idx| {
        const dir = path[0..idx];
        const result = map.getOrPut(dir) catch return;
        if (result.found_existing) {
            result.value_ptr.* += 1;
        } else {
            result.value_ptr.* = 1;
        }
    }
}

fn showFlatView(result: *const DiffResult) !void {
    printSection("\n=== File Changes ({} total) ===", .{result.getTotalChanges()});
    print("\n", .{});
    
    const max_show = 50;
    
    if (result.added.items.len > 0) {
        printColor(Color.bold_green, "Added ({}):\n", .{result.added.items.len});
        const items = result.added.items[0..@min(max_show, result.added.items.len)];
        for (items) |fc| {
            printColor(Color.green, "  [+] {s}", .{fc.path});
            if (fc.size > 0) {
                printColor(Color.gray, " ({s})", .{formatBytes(fc.size)});
            }
            print("\n", .{});
        }
        if (result.added.items.len > max_show) {
            printColor(Color.gray, "  ... and {} more\n", .{result.added.items.len - max_show});
        }
    }
    
    if (result.deleted.items.len > 0) {
        printColor(Color.bold_red, "\nDeleted ({}):\n", .{result.deleted.items.len});
        const items = result.deleted.items[0..@min(max_show, result.deleted.items.len)];
        for (items) |fc| {
            printColor(Color.red, "  [-] {s}", .{fc.path});
            if (fc.size > 0) {
                printColor(Color.gray, " ({s})", .{formatBytes(fc.size)});
            }
            print("\n", .{});
        }
        if (result.deleted.items.len > max_show) {
            printColor(Color.gray, "  ... and {} more\n", .{result.deleted.items.len - max_show});
        }
    }
    
    if (result.modified.items.len > 0) {
        printColor(Color.bold_yellow, "\nModified ({}):\n", .{result.modified.items.len});
        const items = result.modified.items[0..@min(max_show, result.modified.items.len)];
        for (items) |fc| {
            printColor(Color.yellow, "  [M] {s}", .{fc.path});
            if (fc.size > 0) {
                printColor(Color.gray, " ({s})", .{formatBytes(fc.size)});
            }
            print("\n", .{});
        }
        if (result.modified.items.len > max_show) {
            printColor(Color.gray, "  ... and {} more\n", .{result.modified.items.len - max_show});
        }
    }
}

const TreeNode = struct {
    name: []const u8,
    changes: std.ArrayList(FileChange),
    children: std.StringHashMap(*TreeNode),
    
    fn init(allocator: Allocator, name: []const u8) TreeNode {
        return .{
            .name = name,
            .changes = std.ArrayList(FileChange).init(allocator),
            .children = std.StringHashMap(*TreeNode).init(allocator),
        };
    }
    
    fn deinit(self: *TreeNode, allocator: Allocator) void {
        self.changes.deinit();
        var it = self.children.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit(allocator);
            allocator.destroy(entry.value_ptr.*);
        }
        self.children.deinit();
    }
};

fn showTreeView(allocator: Allocator, result: *const DiffResult) !void {
    printSection("\n=== File Changes (Tree View) ===", .{});
    print("\n", .{});
    
    var root = TreeNode.init(allocator, "/");
    defer root.deinit(allocator);
    
    // Build tree
    for (result.added.items) |fc| {
        try addToTree(allocator, &root, fc);
    }
    for (result.deleted.items) |fc| {
        try addToTree(allocator, &root, fc);
    }
    for (result.modified.items) |fc| {
        try addToTree(allocator, &root, fc);
    }
    
    // Print tree
    try printTree(&root, "", true);
}

fn addToTree(allocator: Allocator, root: *TreeNode, fc: FileChange) !void {
    var parts = mem.tokenizeScalar(u8, fc.path, '/');
    var current = root;
    
    var path_parts = std.ArrayList([]const u8).init(allocator);
    defer path_parts.deinit();
    
    while (parts.next()) |part| {
        try path_parts.append(part);
    }
    
    for (path_parts.items, 0..) |part, i| {
        if (i == path_parts.items.len - 1) {
            // Leaf node - add the file
            try current.changes.append(fc);
        } else {
            // Directory node
            const result = try current.children.getOrPut(part);
            if (!result.found_existing) {
                const node = try allocator.create(TreeNode);
                node.* = TreeNode.init(allocator, part);
                result.value_ptr.* = node;
            }
            current = result.value_ptr.*;
        }
    }
}

fn printTree(node: *const TreeNode, prefix: []const u8, is_last: bool) !void {
    // Sort children
    var sorted_keys = std.ArrayList([]const u8).init(std.heap.page_allocator);
    defer sorted_keys.deinit();
    
    var it = node.children.iterator();
    while (it.next()) |entry| {
        try sorted_keys.append(entry.key_ptr.*);
    }
    
    std.sort.insertion([]const u8, sorted_keys.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return mem.order(u8, a, b) == .lt;
        }
    }.lessThan);
    
    // Print current node if not root
    if (!mem.eql(u8, node.name, "/")) {
        print("{s}", .{prefix});
        if (is_last) {
            print("└── ", .{});
        } else {
            print("├── ", .{});
        }
        printColor(Color.blue, "{s}/\n", .{node.name});
    }
    
    // Print files in this directory
    for (node.changes.items) |fc| {
        // Build the sub_prefix
        if (!mem.eql(u8, node.name, "/")) {
            print("{s}", .{prefix});
            if (is_last) {
                print("    ", .{});
            } else {
                print("│   ", .{});
            }
        }
        
        print("├── ", .{});
        
        const file_name = if (mem.lastIndexOfScalar(u8, fc.path, '/')) |idx|
            fc.path[idx + 1 ..]
        else
            fc.path;
        
        switch (fc.change_type) {
            .added => printColor(Color.green, "[+] {s}", .{file_name}),
            .deleted => printColor(Color.red, "[-] {s}", .{file_name}),
            .modified => printColor(Color.yellow, "[M] {s}", .{file_name}),
        }
        
        if (fc.size > 0) {
            const size_str = formatBytes(fc.size);
            defer if (size_str.ptr != "? B".ptr) std.heap.page_allocator.free(size_str);
            printColor(Color.gray, " ({s})", .{size_str});
        }
        print("\n", .{});
    }
    
    // Print subdirectories
    for (sorted_keys.items, 0..) |key, i| {
        const child = node.children.get(key).?;
        
        // Build the child_prefix
        var child_prefix_buf: [1024]u8 = undefined;
        const child_prefix = if (mem.eql(u8, node.name, "/"))
            child_prefix_buf[0..0]
        else if (is_last)
            std.fmt.bufPrint(&child_prefix_buf, "{s}    ", .{prefix}) catch continue
        else
            std.fmt.bufPrint(&child_prefix_buf, "{s}│   ", .{prefix}) catch continue;
        
        const child_is_last = i == sorted_keys.items.len - 1;
        try printTree(child, child_prefix, child_is_last);
    }
}

fn compareDirWorker(
    allocator: Allocator,
    path1: []const u8,
    path2: []const u8,
    rel: []const u8,
    result: *DiffResult,
    mutex: *Thread.Mutex,
) void {
    const full_path1 = std.fmt.allocPrint(allocator, "{s}{s}", .{ path1, rel }) catch return;
    defer allocator.free(full_path1);
    const full_path2 = std.fmt.allocPrint(allocator, "{s}{s}", .{ path2, rel }) catch return;
    defer allocator.free(full_path2);
    
    const d1 = fs.openDirAbsolute(full_path1, .{ .iterate = true }) catch return;
    const d2 = fs.openDirAbsolute(full_path2, .{ .iterate = true }) catch return;
    
    var map1 = std.StringHashMap(std.fs.File.Stat).init(allocator);
    var map2 = std.StringHashMap(std.fs.File.Stat).init(allocator);
    defer {
        var it1 = map1.iterator();
        while (it1.next()) |e| allocator.free(e.key_ptr.*);
        map1.deinit();
        
        var it2 = map2.iterator();
        while (it2.next()) |e| allocator.free(e.key_ptr.*);
        map2.deinit();
    }
    
    // Build map of first snapshot
    var it1 = d1.iterate();
    while (it1.next() catch null) |e| {
        if (e.kind != .file) continue;
        
        const name = allocator.dupe(u8, e.name) catch continue;
        errdefer allocator.free(name);
        
        const stat = d1.statFile(e.name) catch {
            allocator.free(name);
            continue;
        };
        
        if (stat.kind == .sym_link) {
            allocator.free(name);
            continue;
        }
        
        const gop = map1.getOrPut(name) catch {
            allocator.free(name);
            continue;
        };
        
        if (gop.found_existing) {
            allocator.free(name);
        } else {
            gop.value_ptr.* = stat;
        }
    }
    
    // Build map of second snapshot
    var it2 = d2.iterate();
    while (it2.next() catch null) |e| {
        if (e.kind != .file) continue;
        
        const stat = d2.statFile(e.name) catch continue;
        if (stat.kind == .sym_link) continue;
        
        const name = allocator.dupe(u8, e.name) catch continue;
        const gop = map2.getOrPut(name) catch {
            allocator.free(name);
            continue;
        };
        
        if (gop.found_existing) {
            allocator.free(name);
        } else {
            gop.value_ptr.* = stat;
        }
    }
    
    var local_added = std.ArrayList(FileChange).init(allocator);
    var local_deleted = std.ArrayList(FileChange).init(allocator);
    var local_modified = std.ArrayList(FileChange).init(allocator);
    defer local_added.deinit();
    defer local_deleted.deinit();
    defer local_modified.deinit();
    
    var local_added_size: u64 = 0;
    var local_deleted_size: u64 = 0;
    var local_modified_size: u64 = 0;
    
    // Find added and modified files
    var iter = map2.iterator();
    while (iter.next()) |e| {
        const full_path = std.fmt.allocPrint(allocator, "{s}/{s}", .{ rel, e.key_ptr.* }) catch continue;
        errdefer allocator.free(full_path);
        
        if (map1.get(e.key_ptr.*)) |stat1| {
            if (stat1.size != e.value_ptr.*.size or stat1.mtime != e.value_ptr.*.mtime) {
                local_modified.append(.{
                    .path = full_path,
                    .change_type = .modified,
                    .size = e.value_ptr.*.size,
                }) catch {
                    allocator.free(full_path);
                    continue;
                };
                local_modified_size += e.value_ptr.*.size;
            } else {
                allocator.free(full_path);
            }
        } else {
            local_added.append(.{
                .path = full_path,
                .change_type = .added,
                .size = e.value_ptr.*.size,
            }) catch {
                allocator.free(full_path);
                continue;
            };
            local_added_size += e.value_ptr.*.size;
        }
    }
    
    // Find deleted files
    var iter1 = map1.iterator();
    while (iter1.next()) |e| {
        if (!map2.contains(e.key_ptr.*)) {
            const full_path = std.fmt.allocPrint(allocator, "{s}/{s}", .{ rel, e.key_ptr.* }) catch continue;
            local_deleted.append(.{
                .path = full_path,
                .change_type = .deleted,
                .size = e.value_ptr.*.size,
            }) catch {
                allocator.free(full_path);
                continue;
            };
            local_deleted_size += e.value_ptr.*.size;
        }
    }
    
    // Merge results
    if (local_added.items.len > 0 or local_deleted.items.len > 0 or local_modified.items.len > 0) {
        mutex.lock();
        defer mutex.unlock();
        
        result.added.appendSlice(local_added.items) catch {};
        result.deleted.appendSlice(local_deleted.items) catch {};
        result.modified.appendSlice(local_modified.items) catch {};
        
        result.total_added_size += local_added_size;
        result.total_deleted_size += local_deleted_size;
        result.total_modified_size += local_modified_size;
        
        // Clear local arrays without freeing paths (ownership transferred)
        local_added.items.len = 0;
        local_deleted.items.len = 0;
        local_modified.items.len = 0;
    } else {
        // Free paths if not transferring
        for (local_added.items) |fc| allocator.free(fc.path);
        for (local_deleted.items) |fc| allocator.free(fc.path);
        for (local_modified.items) |fc| allocator.free(fc.path);
    }
}

fn analyzePackageChanges(allocator: Allocator, result: *const DiffResult) !void {
    var pkgs = std.StringHashMap(PackageAction).init(allocator);
    defer pkgs.deinit();
    
    // Process added packages
    for (result.added.items) |fc| {
        if (mem.indexOf(u8, fc.path, "/var/lib/pacman/local/")) |idx| {
            const name = fc.path[idx + 22 ..];
            if (extractPackageName(name)) |pkg_name| {
                try pkgs.put(pkg_name, .added);
            }
        }
    }
    
    // Process removed packages
    for (result.deleted.items) |fc| {
        if (mem.indexOf(u8, fc.path, "/var/lib/pacman/local/")) |idx| {
            const name = fc.path[idx + 22 ..];
            if (extractPackageName(name)) |pkg_name| {
                if (pkgs.get(pkg_name)) |existing| {
                    if (existing == .added) {
                        try pkgs.put(pkg_name, .updated);
                    }
                } else {
                    try pkgs.put(pkg_name, .removed);
                }
            }
        }
    }
    
    // Process modified packages
    for (result.modified.items) |fc| {
        if (mem.indexOf(u8, fc.path, "/var/lib/pacman/local/")) |idx| {
            const name = fc.path[idx + 22 ..];
            if (extractPackageName(name)) |pkg_name| {
                _ = pkgs.get(pkg_name) orelse try pkgs.put(pkg_name, .updated);
            }
        }
    }
    
    if (pkgs.count() == 0) return;
    
    printSection("\n=== Package Changes ===", .{});
    print("\n", .{});
    
    const PkgItem = struct {
        name: []const u8,
        action: PackageAction,
    };
    
    var pkg_list = std.ArrayList(PkgItem).init(allocator);
    defer pkg_list.deinit();
    
    var it = pkgs.iterator();
    while (it.next()) |e| {
        try pkg_list.append(.{ .name = e.key_ptr.*, .action = e.value_ptr.* });
    }
    
    std.sort.insertion(PkgItem, pkg_list.items, {}, struct {
        fn lessThan(_: void, x: PkgItem, y: PkgItem) bool {
            return mem.order(u8, x.name, y.name) == .lt;
        }
    }.lessThan);
    
    var installed_count: usize = 0;
    var removed_count: usize = 0;
    var updated_count: usize = 0;
    
    // Group by action type for cleaner display
    for (pkg_list.items) |pkg| {
        if (pkg.action == .added) installed_count += 1;
    }
    for (pkg_list.items) |pkg| {
        if (pkg.action == .removed) removed_count += 1;
    }
    for (pkg_list.items) |pkg| {
        if (pkg.action == .updated) updated_count += 1;
    }
    
    if (installed_count > 0) {
        printColor(Color.bold_green, "Installed ({}):\n", .{installed_count});
        for (pkg_list.items) |pkg| {
            if (pkg.action == .added) {
                printColor(Color.green, "  [+] {s}\n", .{pkg.name});
            }
        }
    }
    
    if (removed_count > 0) {
        if (installed_count > 0) print("\n", .{});
        printColor(Color.bold_red, "Removed ({}):\n", .{removed_count});
        for (pkg_list.items) |pkg| {
            if (pkg.action == .removed) {
                printColor(Color.red, "  [-] {s}\n", .{pkg.name});
            }
        }
    }
    
    if (updated_count > 0) {
        if (installed_count > 0 or removed_count > 0) print("\n", .{});
        printColor(Color.bold_yellow, "Updated ({}):\n", .{updated_count});
        for (pkg_list.items) |pkg| {
            if (pkg.action == .updated) {
                printColor(Color.yellow, "  [↑] {s}\n", .{pkg.name});
            }
        }
    }
    
    print("\n", .{});
    printInfo("Package Summary: ", .{});
    if (installed_count > 0) {
        printColor(Color.green, "{} installed", .{installed_count});
        if (removed_count > 0 or updated_count > 0) print(", ", .{});
    }
    if (removed_count > 0) {
        printColor(Color.red, "{} removed", .{removed_count});
        if (updated_count > 0) print(", ", .{});
    }
    if (updated_count > 0) {
        printColor(Color.yellow, "{} updated", .{updated_count});
    }
    print("\n", .{});
}

fn extractPackageName(path: []const u8) ?[]const u8 {
    const end = mem.indexOfScalar(u8, path, '/') orelse path.len;
    if (end == 0) return null;
    
    // Work backwards to find version separator
    var i = end;
    var dash_count: u8 = 0;
    while (i > 0) : (i -= 1) {
        if (path[i - 1] == '-') {
            if (i < end and path[i] >= '0' and path[i] <= '9') {
                dash_count += 1;
                if (dash_count >= 2) {
                    return path[0 .. i - 1];
                }
            }
        }
    }
    
    return null;
}