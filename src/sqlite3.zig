const std = @import("std");
const builtin = @import("builtin");

const generaldb = @import("db.zig");
const Error = generaldb.Error;
const Db = generaldb.Db;
const QueryArg = generaldb.QueryArg;
const Query = generaldb.Query;
const DbVTable = generaldb.DbVTable;
const ValueType = generaldb.ValueType;
const QueryCol = generaldb.QueryCol;
const QueryRow = generaldb.QueryRow;

const is64 = @sizeOf(usize) == 8;

pub const SqliteDb = struct {
    allocator: std.mem.Allocator,

    file_name: [:0]const u8,
    flags: usize,
    vfs: ?[]const u8,
    sqlite_conn: *allowzero anyopaque,
    stmt: *allowzero anyopaque,
    row_fetched: usize = 0,
    last_errcode: usize,
    last_errmsg: std.ArrayList(u8),
    next_prepare_opts: struct {
        prep_flags: ?usize = null,
        tail: ?*[]const u8 = null,
    } = .{},

    pub fn init(allocator: std.mem.Allocator, file_name: []const u8, opts: struct {
        flags: usize = sqlite3.SQLITE_OPEN_READWRITE | sqlite3.SQLITE_OPEN_CREATE,
        vfs: ?[:0]const u8 = null,
    }) !SqliteDb {
        return SqliteDb{
            .allocator = allocator,
            .file_name = try allocator.dupeZ(u8, file_name),
            .flags = opts.flags,
            .vfs = opts.vfs,
            .sqlite_conn = @ptrFromInt(0),
            .stmt = @ptrFromInt(0),
            .last_errcode = 0,
            .last_errmsg = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn deinit(this: *SqliteDb) void {
        this.allocator.free(this.file_name);
        this.last_errmsg.deinit();
    }

    pub fn getDb(this: *SqliteDb) Db {
        return Db{
            .ptr = this,
            .vtable = .{
                .open = open,
                .close = close,
                .rawQueryEvaluate = rawQueryEvaluate,
                .rawQueryNextRow = rawQueryNextRow,
                .rawQueryNextCol = rawQueryNextCol,
            },
        };
    }

    pub fn getLastErrorMsg(this: *SqliteDb) void {
        this.last_errmsg.clearRetainingCapacity();
        const cmsg = sqlite3.sqlite3_errmsg(this.sqlite_conn);
        const cmsg_s = std.mem.sliceTo(cmsg, 0);
        this.last_errmsg.writer().print("{s}", .{cmsg_s}) catch {};
        std.debug.print("err: {s}\n", .{this.last_errmsg.items});
    }

    fn open(ctx: *anyopaque) Error!void {
        var s: *SqliteDb = @ptrCast(@alignCast(ctx));
        s.last_errcode = sqlite3.sqlite3_open_v2(
            s.file_name.ptr,
            &s.sqlite_conn,
            s.flags,
            if (s.vfs) |vfs| vfs.ptr else 0,
        );
        if (s.last_errcode != sqlite3.SQLITE_OK) {
            s.getLastErrorMsg();
            return error.DbConnFailed;
        }
    }

    fn close(ctx: *anyopaque) void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = sqlite3.sqlite3_close(s.sqlite_conn);
    }

    fn rawQueryEvaluate(ctx: *anyopaque, query: *Query) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));

        const ret = sqlite3.sqlite3_prepare_v3(
            s.sqlite_conn,
            query.raw_query.ptr,
            query.raw_query.len,
            if (s.next_prepare_opts.prep_flags) |pf| pf else 0,
            &s.stmt,
            if (s.next_prepare_opts.tail) |t| @ptrCast(t) else @ptrFromInt(0),
        );
        if (ret != sqlite3.SQLITE_OK) {
            s.getLastErrorMsg();
            return error.StmtPrepareFailed;
        }
        errdefer {
            _ = sqlite3.sqlite3_finalize(s.stmt);
            query.finalized = true;
        }
        query.col_count = sqlite3.sqlite3_column_count(s.stmt);
        query.evaluated = true;
    }

    fn rawQueryNextRow(ctx: *anyopaque) Error!?usize {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        const ret = sqlite3.sqlite3_step(s.stmt);
        switch (ret) {
            sqlite3.SQLITE_ROW => {
                const row_seqid = s.row_fetched;
                s.row_fetched += 1;
                return row_seqid;
            },
            sqlite3.SQLITE_DONE => {
                return null;
            },
            sqlite3.SQLITE_BUSY => {
                return error.StmtFetchRowBusy;
            },
            sqlite3.SQLITE_ERROR => {
                s.getLastErrorMsg();
                return error.StmtFetchRowFailed;
            },
            sqlite3.SQLITE_MISUSE => {
                return error.StmtFetchRowMisuse;
            },
            else => {
                std.debug.print("ret = {d}\n", .{ret});
                @panic("handle me!");
            },
        }
    }

    fn rawQueryNextCol(ctx: *anyopaque, row: *QueryRow, col_idx: usize) Error!?QueryCol {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));

        const name = brk: {
            const cname = sqlite3.sqlite3_column_name(s.stmt, col_idx);
            if (cname == null) {
                break :brk try s.allocator.dupe(u8, "");
            } else {
                const cname_s = std.mem.sliceTo(cname, 0);
                break :brk try s.allocator.dupe(u8, cname_s);
            }
        };

        switch (sqlite3.sqlite3_column_type(s.stmt, col_idx)) {
            sqlite3.SQLITE_INTEGER => {
                const value = brk: {
                    if (is64) {
                        const i64v = sqlite3.sqlite3_column_int64(s.stmt, col_idx);
                        break :brk ValueType{ .INT64 = i64v };
                    } else {
                        const i32v = sqlite3.sqlite3_column_int(s.stmt, col_idx);
                        break :brk ValueType{ .INT32 = i32v };
                    }
                };
                return QueryCol{
                    .allocator = s.allocator,
                    .row = row,
                    .name = name,
                    .value = value,
                };
            },
            sqlite3.SQLITE_FLOAT => {
                const f64v = sqlite3.sqlite3_column_double(s.stmt, col_idx);
                return QueryCol{
                    .allocator = s.allocator,
                    .row = row,
                    .name = name,
                    .value = ValueType{ .FLOAT64 = f64v },
                };
            },
            sqlite3.SQLITE3_TEXT => {
                const len = sqlite3.sqlite3_column_bytes(s.stmt, col_idx);
                const ctext = sqlite3.sqlite3_column_text(s.stmt, col_idx);
                return QueryCol{
                    .allocator = s.allocator,
                    .row = row,
                    .name = name,
                    .value = ValueType{ .TEXT = try s.allocator.dupe(u8, ctext[0..len]) },
                };
            },
            sqlite3.SQLITE_BLOB => {
                const len = sqlite3.sqlite3_column_bytes(s.stmt, col_idx);
                const cblob = sqlite3.sqlite3_column_blob(s.stmt, col_idx);
                return QueryCol{
                    .allocator = s.allocator,
                    .row = row,
                    .name = name,
                    .value = ValueType{ .BLOB = try s.allocator.dupe(u8, cblob[0..len]) },
                };
            },
            sqlite3.SQLITE_NULL => {
                return QueryCol{
                    .allocator = s.allocator,
                    .row = row,
                    .name = name,
                    .value = ValueType{ .NULL = undefined },
                };
            },
            else => unreachable,
        }
    }

    // extern interfaces of sqlite3
    const sqlite3 = struct {
        // error codes
        pub const SQLITE_OK = 0; // /* Successful result */
        pub const SQLITE_ERROR = 1; // /* Generic error */
        pub const SQLITE_INTERNAL = 2; // /* Internal logic error in SQLite */
        pub const SQLITE_PERM = 3; // /* Access permission denied */
        pub const SQLITE_ABORT = 4; // /* Callback routine requested an abort */
        pub const SQLITE_BUSY = 5; // /* The database file is locked */
        pub const SQLITE_LOCKED = 6; // /* A table in the database is locked */
        pub const SQLITE_NOMEM = 7; // /* A malloc() failed */
        pub const SQLITE_READONLY = 8; // /* Attempt to write a readonly database */
        pub const SQLITE_INTERRUPT = 9; // /* Operation terminated by sqlite3_interrupt()*/
        pub const SQLITE_IOERR = 10; // /* Some kind of disk I/O error occurred */
        pub const SQLITE_CORRUPT = 11; // /* The database disk image is malformed */
        pub const SQLITE_NOTFOUND = 12; // /* Unknown opcode in sqlite3_file_control() */
        pub const SQLITE_FULL = 13; // /* Insertion failed because database is full */
        pub const SQLITE_CANTOPEN = 14; // /* Unable to open the database file */
        pub const SQLITE_PROTOCOL = 15; // /* Database lock protocol error */
        pub const SQLITE_EMPTY = 16; // /* Internal use only */
        pub const SQLITE_SCHEMA = 17; // /* The database schema changed */
        pub const SQLITE_TOOBIG = 18; // /* String or BLOB exceeds size limit */
        pub const SQLITE_CONSTRAINT = 19; // /* Abort due to constraint violation */
        pub const SQLITE_MISMATCH = 20; // /* Data type mismatch */
        pub const SQLITE_MISUSE = 21; // /* Library used incorrectly */
        pub const SQLITE_NOLFS = 22; // /* Uses OS features not supported on host */
        pub const SQLITE_AUTH = 23; // /* Authorization denied */
        pub const SQLITE_FORMAT = 24; // /* Not used */
        pub const SQLITE_RANGE = 25; // /* 2nd parameter to sqlite3_bind out of range */
        pub const SQLITE_NOTADB = 26; // /* File opened that is not a database file */
        pub const SQLITE_NOTICE = 27; // /* Notifications from sqlite3_log() */
        pub const SQLITE_WARNING = 28; // /* Warnings from sqlite3_log() */
        pub const SQLITE_ROW = 100; // /* sqlite3_step() has another row ready */
        pub const SQLITE_DONE = 101; // /* sqlite3_step() has finished executing */

        // OPEN flags
        pub const SQLITE_OPEN_READONLY = 0x00000001; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_READWRITE = 0x00000002; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_CREATE = 0x00000004; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_DELETEONCLOSE = 0x00000008; // /* VFS only */
        pub const SQLITE_OPEN_EXCLUSIVE = 0x00000010; // /* VFS only */
        pub const SQLITE_OPEN_AUTOPROXY = 0x00000020; // /* VFS only */
        pub const SQLITE_OPEN_URI = 0x00000040; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_MEMORY = 0x00000080; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_MAIN_DB = 0x00000100; // /* VFS only */
        pub const SQLITE_OPEN_TEMP_DB = 0x00000200; // /* VFS only */
        pub const SQLITE_OPEN_TRANSIENT_DB = 0x00000400; // /* VFS only */
        pub const SQLITE_OPEN_MAIN_JOURNAL = 0x00000800; // /* VFS only */
        pub const SQLITE_OPEN_TEMP_JOURNAL = 0x00001000; // /* VFS only */
        pub const SQLITE_OPEN_SUBJOURNAL = 0x00002000; // /* VFS only */
        pub const SQLITE_OPEN_SUPER_JOURNAL = 0x00004000; // /* VFS only */
        pub const SQLITE_OPEN_NOMUTEX = 0x00008000; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_FULLMUTEX = 0x00010000; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_SHAREDCACHE = 0x00020000; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_PRIVATECACHE = 0x00040000; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_WAL = 0x00080000; // /* VFS only */
        pub const SQLITE_OPEN_NOFOLLOW = 0x01000000; // /* Ok for sqlite3_open_v2() */
        pub const SQLITE_OPEN_EXRESCODE = 0x02000000; // /* Extended result codes */

        // prepare flags
        pub const SQLITE_PREPARE_PERSISTENT = 0x01;
        pub const SQLITE_PREPARE_NORMALIZE = 0x02;
        pub const SQLITE_PREPARE_NO_VTAB = 0x04;

        // col type
        pub const SQLITE_INTEGER = 1;
        pub const SQLITE_FLOAT = 2;
        pub const SQLITE_BLOB = 4;
        pub const SQLITE_NULL = 5;
        pub const SQLITE3_TEXT = 3;

        extern fn sqlite3_errmsg(pDb: *allowzero anyopaque) [*c]const u8;
        extern fn sqlite3_open_v2(filename: [*c]const u8, ppDb: **allowzero anyopaque, flags: usize, zVfs: [*c]const u8) usize;
        extern fn sqlite3_close(pDb: *allowzero anyopaque) usize;
        extern fn sqlite3_prepare_v3(pDb: *allowzero anyopaque, zSql: [*c]const u8, nByte: usize, prepFlags: usize, ppStmt: **allowzero anyopaque, pzTail: *allowzero [*c]const u8) usize;
        extern fn sqlite3_step(pStmt: *allowzero anyopaque) usize;
        extern fn sqlite3_finalize(pStmt: *allowzero anyopaque) usize;
        extern fn sqlite3_column_count(pStmt: *allowzero anyopaque) usize;
        extern fn sqlite3_column_name(pStmt: *allowzero anyopaque, N: usize) [*c]u8;
        extern fn sqlite3_column_type(pStmt: *allowzero anyopaque, iCol: usize) usize;
        extern fn sqlite3_column_blob(pStmt: *allowzero anyopaque, iCol: usize) [*c]u8;
        extern fn sqlite3_column_double(pStmt: *allowzero anyopaque, iCol: usize) f64;
        extern fn sqlite3_column_int(pStmt: *allowzero anyopaque, iCol: usize) c_int;
        extern fn sqlite3_column_int64(pStmt: *allowzero anyopaque, iCol: usize) i64;
        extern fn sqlite3_column_text(pStmt: *allowzero anyopaque, iCol: usize) [*c]u8;
        extern fn sqlite3_column_bytes(pStmt: *allowzero anyopaque, iCol: usize) usize;
    };
};

test "sqlite3" {
    const testing = std.testing;
    var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
    defer sdb.deinit();
    var db: Db = sdb.getDb();
    try db.open();
    defer db.close();
    var output_buf = std.ArrayList(u8).init(testing.allocator);
    defer output_buf.deinit();
    const output_writer = output_buf.writer();
    {
        const expected_output =
            \\row 0: 1=db.ValueType{ .INT64 = 1 }, "hello"=db.ValueType{ .TEXT = { 104, 101, 108, 108, 111 } }, 5.0=db.ValueType{ .FLOAT64 = 5.0e+00 }, 
            \\
        ;
        output_buf.clearRetainingCapacity();
        var q = try Query.init(std.testing.allocator, &db, "select 1, \"hello\", 5.0", null);
        defer q.deinit();
        var rit = try q.iterator();
        while (true) {
            var maybe_row = try rit.next();
            if (maybe_row) |*row| {
                try output_writer.print("row {d}: ", .{row.row_seqid});
                var cit = row.iterator();
                while (try cit.next()) |col| {
                    defer col.deinit();
                    try output_writer.print("{s}={any}, ", .{ col.name, col.value });
                }
                try output_writer.print("\n", .{});
            } else {
                break;
            }
        }
        try testing.expectEqualSlices(u8, expected_output, output_buf.items);
    }
}
