const std = @import("std");

pub const Error = error{
    DbConnFailed,
    StmtPrepareFailed,
    StmtFetchRowFailed,
    StmtFetchRowBusy,
    StmtFetchRowMisuse,
} || std.mem.Allocator.Error;

// data type

pub const DataType = enum {
    NULL,
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    UINT64,
    FLOAT32,
    FLOAT64,
    TEXT,
    BLOB,
};

// query

pub const ValueType = union(DataType) {
    NULL: void,
    INT8: i8,
    UINT8: u8,
    INT16: i16,
    UINT16: u16,
    INT32: i32,
    UINT32: u32,
    INT64: i64,
    UINT64: u64,
    FLOAT32: f32,
    FLOAT64: f64,
    TEXT: []const u8,
    BLOB: []const u8,

    pub fn free(this: ValueType, allocator: std.mem.Allocator) void {
        switch (this) {
            DataType.TEXT => allocator.free(this.TEXT),
            DataType.BLOB => allocator.free(this.BLOB),
            else => {},
        }
    }
};

pub const QueryArg = ValueType;

pub const Query = struct {
    pub const Iterator = struct {
        q: *Query,
        fetched_row_count: usize = 0,

        pub fn next(this: *Iterator) !?QueryRow {
            if (try this.q.db.rawQueryNextRow()) |row_seqid| {
                this.fetched_row_count += 1;
                this.q.row_affected += 1;
                return QueryRow{
                    .q = this.q,
                    .row_seqid = row_seqid,
                };
            } else {
                return null;
            }
        }
    };

    allocator: std.mem.Allocator,
    db: *Db,
    raw_query: [:0]const u8,
    raw_bind_args: ?[]const QueryArg,
    evaluated: bool = false,
    finalized: bool = false,
    col_count: usize = 0,
    row_affected: usize = 0,

    pub fn init(allocator: std.mem.Allocator, db: *Db, query: []const u8, bind_args: ?[]const QueryArg) !Query {
        return .{
            .allocator = allocator,
            .db = db,
            .raw_query = try allocator.dupeZ(u8, query),
            .raw_bind_args = bind_args_brk: {
                if (bind_args) |ba| {
                    break :bind_args_brk try allocator.dupe(QueryArg, ba);
                } else {
                    break :bind_args_brk null;
                }
            },
        };
    }

    pub fn deinit(this: *Query) void {
        this.allocator.free(this.raw_query);
        if (this.raw_bind_args) |rba| {
            this.allocator.free(rba);
        }
    }

    /// return row iterator
    pub fn iterator(this: *Query) !Iterator {
        if (!this.evaluated) {
            try this.db.rawQueryEvaluate(this);
        }
        return Iterator{ .q = this };
    }

    pub fn finalize(this: *Query) void {
        if (this.evaluated) {
            _ = this.db.finalizeQuery();
        }
    }
};

pub const QueryRow = struct {
    pub const Iterator = struct {
        row: *QueryRow,
        fetched_col_count: usize,

        pub fn next(this: *Iterator) !?QueryCol {
            const to_fetch_col_idx = this.fetched_col_count;
            if (to_fetch_col_idx >= this.row.q.col_count) {
                return null;
            }
            const maybe_colpair = try this.row.q.db.rawQueryNextCol(this.row, to_fetch_col_idx);
            if (maybe_colpair) |colpair| {
                this.fetched_col_count += 1;
                return QueryCol{
                    .allocator = colpair.allocator,
                    .row = this.row,
                    .name = colpair.name,
                    .value = colpair.value,
                };
            } else {
                return null;
            }
        }
    };

    q: *Query,
    row_seqid: usize,

    /// return col iterator
    pub fn iterator(this: *QueryRow) Iterator {
        return Iterator{ .row = this, .fetched_col_count = 0 };
    }
};

pub const QueryCol = struct {
    allocator: std.mem.Allocator,
    row: *QueryRow,
    name: []const u8,
    value: ValueType,

    pub fn deinit(this: *const QueryCol) void {
        this.value.free(this.allocator);
        this.allocator.free(this.name);
    }
};

// db

pub const DbVTable = struct {
    open: *const fn (ctx: *anyopaque) Error!void,
    close: *const fn (ctx: *anyopaque) void,
    rawQueryEvaluate: *const fn (ctx: *anyopaque, query: *Query) Error!void,
    rawQueryNextRow: *const fn (ctx: *anyopaque) Error!?usize,
    rawQueryNextCol: *const fn (ctx: *anyopaque, row: *QueryRow, col_idx: usize) Error!?QueryCol,
};

pub const Db = struct {
    ptr: *anyopaque,
    vtable: DbVTable,

    pub inline fn open(this: *Db) Error!void {
        try this.vtable.open(this.ptr);
    }

    pub inline fn close(this: *Db) void {
        this.vtable.close(this.ptr);
    }

    pub inline fn rawQueryEvaluate(this: *Db, query: *Query) Error!void {
        try this.vtable.rawQueryEvaluate(this.ptr, query);
    }

    pub inline fn rawQueryNextRow(this: *Db) Error!?usize {
        return this.vtable.rawQueryNextRow(this.ptr);
    }

    pub inline fn rawQueryNextCol(this: *Db, row: *QueryRow, col_idx: usize) Error!?QueryCol {
        return this.vtable.rawQueryNextCol(this.ptr, row, col_idx);
    }
};
