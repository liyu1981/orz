const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;

const generalDb = @import("db.zig");
const Db = generalDb;
const Error = generalDb.Error;
const QueryArg = generalDb.QueryArg;
const Query = generalDb.Query;
const DbVTable = generalDb.DbVTable;
const ValueType = generalDb.ValueType;
const QueryCol = generalDb.QueryCol;
const QueryRow = generalDb.QueryRow;
const Migration = generalDb.Migration;
const DataType = generalDb.DataType;
const ChangeSet = generalDb.ChangeSet;
const SchemaUtil = generalDb.SchemaUtil;
const RelationOpts = generalDb.RelationOpts;
const IndexOpts = generalDb.IndexOpts;
const Constraint = generalDb.Constraint;
const TableSchema = generalDb.TableSchema;
const ColOpts = generalDb.ColOpts;

const is_x64 = @sizeOf(usize) == 8;

pub const SqliteDb = struct {
    allocator: std.mem.Allocator,

    database_name: []const u8,
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
        const duped_file_name = try allocator.dupeZ(u8, file_name);
        return SqliteDb{
            .allocator = allocator,
            .database_name = duped_file_name,
            .file_name = duped_file_name,
            .flags = opts.flags,
            .vfs = opts.vfs,
            .sqlite_conn = @ptrFromInt(0),
            .stmt = @ptrFromInt(0),
            .last_errcode = 0,
            .last_errmsg = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn deinit(this: *SqliteDb) void {
        // database_name points to same mem of file_name, no free
        this.allocator.free(this.file_name);
        this.last_errmsg.deinit();
    }

    pub fn getDb(this: *SqliteDb) Db {
        return Db{
            .ctx = this,
            .vtable = .{
                .implOpen = open,
                .implClose = close,
                .implCreateDatabase = createDatabase,
                .implUseDatabase = useDatabase,
                .implQueryEvaluate = queryEvaluate,
                .implQueryNextRow = queryNextRow,
                .implQueryNextCol = queryNextCol,
                .implQueryFinalize = queryFinalize,
                .implMigrate = migrate,
                .implCreateTable = createTable,
                .implDropTable = dropTable,
                .implRenameTable = renameTable,
                .implHasTable = hasTable,
                .implAddColumn = addColumn,
                .implDropColumn = dropColumn,
                .implRenameColumn = renameColumn,
                .implAlterColumn = alterColumn,
                .implCreateConstraint = createConstraint,
                .implDropConstraint = dropConstraint,
                .implCreateIndex = createIndex,
                .implDropIndex = dropIndex,
                .implRenameIndex = renameIndex,
                .implHasIndex = hasIndex,
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

    // utils

    fn checkBindResultOk(ret: usize) !void {
        switch (ret) {
            sqlite3.SQLITE_OK => {},
            sqlite3.SQLITE_MISUSE => {
                return error.StmtBindMisuse;
            },
            sqlite3.SQLITE_TOOBIG => {
                return error.StmtBindTooBig;
            },
            sqlite3.SQLITE_NOMEM => {
                return error.StmtBindNomem;
            },
            sqlite3.SQLITE_RANGE => {
                return error.StmtBindRange;
            },
            else => {
                std.debug.print("ret = {d}\n", .{ret});
                @panic("TODO: handle this!");
            },
        }
    }

    fn valueTypeToSqlite3Type(vt: ValueType) []const u8 {
        switch (vt) {
            .NULL => return "NULL",
            .BOOL => return "INTEGER",
            .INT8, .UINT8, .INT16, .UINT16, .INT32, .UINT32, .INT64, .UINT64 => return "INTEGER",
            .FLOAT32, .FLOAT64 => return "REAL",
            .TEXT => return "TEXT",
            .BLOB => return "BLOB",
        }
    }

    fn valueType2Sqlite3DefaultValueStr(writer: std.ArrayList(u8).Writer, vt: ValueType) !void {
        switch (vt) {
            .NULL => unreachable,
            .BOOL => if (vt.BOOL) try writer.print("1", .{}) else try writer.print("0", .{}),
            .INT8, .INT16, .INT32, .INT64 => |iv| try writer.print("{d}", .{iv}),
            .UINT8, .UINT16, .UINT32, .UINT64 => |iv| try writer.print("{d}", .{iv}),
            .FLOAT32, .FLOAT64 => |fv| try writer.print("{d}", .{fv}),
            .TEXT => |tv| try writer.print("'{s}'", .{tv}),
            .BLOB => unreachable,
        }
    }

    // vtable methods

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
        s.last_errcode = sqlite3.sqlite3_close(s.sqlite_conn);
    }

    fn createDatabase(ctx: *anyopaque, name: []const u8, opts: DbVTable.CreateDatabaseOpts) Error!void {
        _ = opts;
        // in sqlite this does not make sense, so simply redirect to useDatabase
        try useDatabase(ctx, name);
    }

    fn useDatabase(ctx: *anyopaque, name: []const u8) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        // in sqlite this does not make sense, so only check name match or not
        if (!std.mem.eql(u8, s.database_name, name)) {
            return error.DbUseDatabaseFailed;
        }
    }

    fn queryEvaluate(ctx: *anyopaque, query: *Query) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));

        s.last_errcode = sqlite3.sqlite3_prepare_v3(
            s.sqlite_conn,
            query.raw_query.ptr,
            query.raw_query.len,
            if (s.next_prepare_opts.prep_flags) |pf| pf else 0,
            &s.stmt,
            if (s.next_prepare_opts.tail) |t| @ptrCast(t) else @ptrFromInt(0),
        );
        if (s.last_errcode != sqlite3.SQLITE_OK) {
            s.getLastErrorMsg();
            return error.StmtPrepareFailed;
        }
        errdefer {
            s.last_errcode = sqlite3.sqlite3_finalize(s.stmt);
            query.finalized = true;
        }

        if (query.raw_bind_args) |bind_args| {
            var next_index: usize = 1;
            for (bind_args) |bind_arg| {
                const idx = brk: {
                    if (bind_arg.namez) |namez| {
                        const name_idx = sqlite3.sqlite3_bind_parameter_index(s.stmt, namez.ptr);
                        if (name_idx == 0) {
                            std.debug.print("{s} is not found in stmt: {s}\n", .{ namez, query.raw_query });
                            return error.StmtBindNameNotFound;
                        }
                        break :brk name_idx;
                    } else {
                        const next_idx = next_index;
                        next_index += 1;
                        break :brk next_idx;
                    }
                };

                var ret: usize = 0;
                switch (bind_arg.value) {
                    .NULL => {
                        ret = sqlite3.sqlite3_bind_null(s.stmt, idx);
                        try SqliteDb.checkBindResultOk(ret);
                    },
                    .INT64 => |i64v| {
                        ret = sqlite3.sqlite3_bind_int64(s.stmt, idx, i64v);
                        try SqliteDb.checkBindResultOk(ret);
                    },
                    .FLOAT64 => |f64v| {
                        ret = sqlite3.sqlite3_bind_double(s.stmt, idx, f64v);
                        try SqliteDb.checkBindResultOk(ret);
                    },
                    .TEXT => |textv| {
                        ret = sqlite3.sqlite3_bind_text(s.stmt, idx, textv.ptr, textv.len, sqlite3.SQLITE_STATIC);
                        try SqliteDb.checkBindResultOk(ret);
                    },
                    .BLOB => |blobv| {
                        ret = sqlite3.sqlite3_bind_blob(s.stmt, idx, blobv.ptr, blobv.len, sqlite3.SQLITE_STATIC);
                        try SqliteDb.checkBindResultOk(ret);
                    },
                    else => unreachable,
                }
            }
        }

        s.row_fetched = 0;
        query.col_count = sqlite3.sqlite3_column_count(s.stmt);
        query.prepared = true;
    }

    fn queryNextRow(ctx: *anyopaque) Error!?usize {
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

    fn queryNextCol(ctx: *anyopaque, row: *QueryRow, col_idx: usize) Error!?QueryCol {
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
                    if (is_x64) {
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
                    .value = ValueType{ .BLOB = try s.allocator.dupe(i8, cblob[0..len]) },
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

    fn queryFinalize(ctx: *anyopaque, query: *Query) usize {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        s.last_errcode = sqlite3.sqlite3_finalize(s.stmt);
        s.stmt = @ptrFromInt(0);
        s.row_fetched = 0;
        query.finalized = true;
        return s.last_errcode;
    }

    fn migrate(ctx: *anyopaque, db: *Db, migration: *Migration, direction: DbVTable.MigrateDirection) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        const maybe_change_set = brk: {
            switch (direction) {
                .up => break :brk try migration.up(),
                .down => break :brk try migration.down(),
            }
        };
        if (maybe_change_set) |*change_set| {
            defer change_set.deinit();
            _ = try db.queryExecuteSlice(s.allocator, "BEGIN TRANSACTION");
            errdefer {
                _ = db.queryExecuteSlice(s.allocator, "ROLLBACK") catch |err| {
                    std.debug.print("Rollback failed: {any}\n", .{err});
                };
            }
            for (0..change_set.query_len) |i| {
                _ = try db.queryExecute(&change_set.query_buf[i]);
            }
            // TODO: need to update meta table
            _ = try db.queryExecuteSlice(s.allocator, "COMMIT");
        }
    }

    fn genColumnDef(sql_writer: std.ArrayList(u8).Writer, table_name: []const u8, column: TableSchema.Column) Error!void {
        try sql_writer.print("'{s}' {s}", .{
            column.name,
            valueTypeToSqlite3Type(column.type),
        });
        if (column.opts.unique) {
            try sql_writer.print(" {s}", .{"UNIQUE"});
        }
        if (!column.opts.nullable) {
            try sql_writer.print(" {s}", .{"NOT NULL"});
        }
        if (column.opts.auto_increment) {
            std.debug.print("auto_increment (for table '{s}' column '{s}') has no effect in sqlite3 to normal column, and should be avoided to id column, see https://www.sqlite.org/autoinc.html. Please leave it with default value (false).\n", .{ table_name, column.name });
            unreachable;
        }
        if (column.opts.collate_method) |cm| {
            try sql_writer.print(" COLLATE {s}", .{cm});
        }
        if (column.opts.default_value) |dv| {
            try sql_writer.print(" DEFAULT ", .{});
            try valueType2Sqlite3DefaultValueStr(sql_writer, dv);
        }
    }

    fn genCreateIndexSql(sql_writer: std.ArrayList(u8).Writer, table_name: []const u8, index: TableSchema.Index) Error!void {
        try sql_writer.print("CREATE", .{});
        if (index.opts.unique) {
            try sql_writer.print(" UNIQUE", .{});
        }
        try sql_writer.print(" INDEX \"{s}\" ON \"{s}\"(", .{ index.name, table_name });
        for (0..index.keys.len) |i| {
            if (i != 0) {
                try sql_writer.print(",", .{});
            }
            try sql_writer.print("'{s}'", .{index.keys[i]});
            switch (index.opts.sortings[i]) {
                .asc => try sql_writer.print(" ASC", .{}),
                .desc => try sql_writer.print(" DESC", .{}),
            }
        }
        try sql_writer.print(")", .{});
    }

    fn createTable(
        ctx: *anyopaque,
        db: *Db,
        change_set: *ChangeSet,
        table_schema: TableSchema,
        opts: DbVTable.CreateTableOpts,
    ) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try sql_writer.print("CREATE TABLE '{s}' (", .{table_schema.table_name});
        // columns
        for (0..table_schema.columns.len) |i| {
            const column = table_schema.columns[i];
            if (i > 0) {
                try sql_writer.print(" ,", .{});
            }
            try genColumnDef(sql_writer, table_schema.table_name, column);
        }
        // primary key
        if (table_schema.primary_key_names.len > 0) {
            try sql_writer.print(" , PRIMARY KEY (", .{});
            for (0..table_schema.primary_key_names.len) |i| {
                if (i != 0) {
                    try sql_writer.print(",", .{});
                }
                try sql_writer.print("'{s}'", .{table_schema.primary_key_names[i]});
            }
            try sql_writer.print(")", .{});
        }
        // foreign keys
        if (table_schema.has_foreign_key) {
            for (table_schema.foreign_keys) |fk| {
                try sql_writer.print(", CONSTRAINT {s} FOREIGN KEY('{s}') REFERENCES '{s}'('{s}')", .{ fk.name, fk.col_name, fk.table_name, fk.table_col_name });
            }
        }
        try sql_writer.print(")", .{});
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });

        // indexes
        if (table_schema.has_indexes) {
            for (table_schema.indexes) |index| {
                sql_buf.clearRetainingCapacity();
                try genCreateIndexSql(sql_writer, table_schema.table_name, index);
                try change_set.append(Query{
                    .allocator = s.allocator,
                    .db = db,
                    .raw_query = try sql_buf.toOwnedSliceSentinel(0),
                });
            }
        }
    }

    fn dropTable(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, opts: DbVTable.DropTableOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try sql_writer.print("DROP TABLE '{s}'", .{table_name});
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });
    }

    fn renameTable(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, from_table_name: []const u8, to_table_name: []const u8, opts: DbVTable.RenameTableOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try sql_writer.print("ALTER TABLE '{s}' RENAME TO '{s}'", .{ from_table_name, to_table_name });
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });
    }

    fn hasTable(ctx: *anyopaque, db: *Db, table_name: []const u8) Error!bool {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try sql_writer.print("SELECT * FROM sqlite_master WHERE type='table' AND name='{s}'", .{table_name});
        const row_affected = try db.queryExecuteSlice(s.allocator, sql_buf.items);
        return row_affected > 0;
    }

    fn addColumn(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, column: TableSchema.Column, opts: DbVTable.AddColumnOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try sql_writer.print("ALTER TABLE '{s}' ADD ", .{table_name});
        try genColumnDef(sql_writer, table_name, column);
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });
    }

    fn dropColumn(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, opts: DbVTable.DropColumnOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try sql_writer.print("ALTER TABLE '{s}' DROP '{s}'", .{ table_name, col_name });
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });
    }

    fn renameColumn(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, from_col_name: []const u8, to_col_name: []const u8, opts: DbVTable.RenameColumnOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try sql_writer.print("ALTER TABLE '{s}' RENAME COLUMN '{s}' TO '{s}'", .{ table_name, from_col_name, to_col_name });
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });
    }

    fn alterColumn(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, new_type: ValueType, new_col_opts: ColOpts, opts: DbVTable.AlterColumnOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        // sqlite3 do not support alter table, so we have to basically create new table and then change and drop table,
        // this is mostly best effort as many moving parts may fail
        const alter_tmp_table_name = brk: {
            try sql_writer.print("{s}_alter_tmp", .{table_name});
            break :brk try sql_buf.toOwnedSlice();
        };
        defer s.allocator.free(alter_tmp_table_name);
        {
            sql_buf.clearRetainingCapacity();
            try sql_writer.print("CREATE TABLE '{s}' AS SELECT * FROM \"{s}\"", .{ alter_tmp_table_name, table_name });
            try change_set.append(Query{
                .allocator = s.allocator,
                .db = db,
                .raw_query = try sql_buf.toOwnedSliceSentinel(0),
            });
        }
        try db.dropColumn(change_set, alter_tmp_table_name, col_name, .{});
        try db.addColumn(
            change_set,
            alter_tmp_table_name,
            TableSchema.Column{ .name = col_name, .type = new_type, .opts = new_col_opts },
            .{},
        );
        {
            sql_buf.clearRetainingCapacity();
            try sql_writer.print("INSERT INTO \"{s}\" (\"{s}\") SELECT \"{s}\" FROM \"{s}\"", .{ alter_tmp_table_name, col_name, col_name, table_name });
            try change_set.append(Query{
                .allocator = s.allocator,
                .db = db,
                .raw_query = try sql_buf.toOwnedSliceSentinel(0),
            });
        }
        try db.dropTable(change_set, table_name, .{});
        try db.renameTable(change_set, alter_tmp_table_name, table_name, .{});
    }

    fn createConstraint(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, constraint: Constraint, opts: DbVTable.CreateConstraintOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        _ = sql_writer;
        _ = table_name;
        _ = constraint;
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });
    }

    fn dropConstraint(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, constraint_name: []const u8, opts: DbVTable.DropConstraintOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        _ = sql_writer;
        _ = table_name;
        _ = constraint_name;
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });
    }

    fn createIndex(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, index: TableSchema.Index, opts: DbVTable.CreateIndexOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try genCreateIndexSql(sql_writer, table_name, index);
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });
    }

    fn dropIndex(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, index_name: []const u8, opts: DbVTable.DropIndexOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try sql_writer.print("DROP INDEX \"{s}\"", .{index_name});
        try change_set.append(Query{
            .allocator = s.allocator,
            .db = db,
            .raw_query = try sql_buf.toOwnedSliceSentinel(0),
        });
    }

    fn renameIndex(ctx: *anyopaque, db: *Db, change_set: *ChangeSet, index_name: []const u8, new_index_name: []const u8, opts: DbVTable.RenameIndexOpts) Error!void {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        _ = opts;
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        // sqlite3 does not support rename index, so here we query the old def of index
        // replace the name, create new one and drop old one
        const orig_create_index_sql = brk: {
            const Result = struct {
                sql: []const u8,
            };
            var record: Result = undefined;
            try sql_writer.print("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = '{s}'", .{index_name});
            var q = try Query.init(s.allocator, db, sql_buf.items, null);
            defer q.deinit();
            var rit = try q.iterator();
            var maybe_row = try rit.next();
            if (maybe_row) |*row| {
                try QueryRow.into(row, Result, &record);
            } else {
                std.debug.print("index '{s}' does not exist!\n", .{index_name});
                unreachable;
            }
            q.finalize();
            break :brk record.sql;
        };
        defer s.allocator.free(orig_create_index_sql);
        // create a new index with different name
        {
            const new_create_index_sql =
                try std.mem.replaceOwned(u8, s.allocator, orig_create_index_sql, index_name, new_index_name);
            defer s.allocator.free(new_create_index_sql);
            const new_create_index_sqlz = try s.allocator.dupeZ(u8, new_create_index_sql);
            try change_set.append(Query{
                .allocator = s.allocator,
                .db = db,
                .raw_query = new_create_index_sqlz,
            });
        }
        // drop the old index
        try db.dropIndex(change_set, index_name, .{});
    }

    fn hasIndex(ctx: *anyopaque, db: *Db, index_name: []const u8) Error!bool {
        const s: *SqliteDb = @ptrCast(@alignCast(ctx));
        var sql_buf = std.ArrayList(u8).init(s.allocator);
        defer sql_buf.deinit();
        const sql_writer = sql_buf.writer();
        try sql_writer.print("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = '{s}'", .{index_name});
        const rows_affected = try db.queryExecuteSlice(s.allocator, sql_buf.items);
        return rows_affected > 0;
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

        // bind api
        pub const SQLITE_STATIC = @as(*allowzero anyopaque, @ptrFromInt(0));

        extern fn sqlite3_errmsg(pDb: *allowzero anyopaque) [*c]const u8;
        extern fn sqlite3_open_v2(filename: [*c]const u8, ppDb: **allowzero anyopaque, flags: usize, zVfs: [*c]const u8) usize;
        extern fn sqlite3_close(pDb: *allowzero anyopaque) usize;
        extern fn sqlite3_prepare_v3(pDb: *allowzero anyopaque, zSql: [*c]const u8, nByte: usize, prepFlags: usize, ppStmt: **allowzero anyopaque, pzTail: *allowzero [*c]const u8) usize;
        extern fn sqlite3_step(pStmt: *allowzero anyopaque) usize;
        extern fn sqlite3_finalize(pStmt: *allowzero anyopaque) usize;
        extern fn sqlite3_column_count(pStmt: *allowzero anyopaque) usize;
        extern fn sqlite3_column_name(pStmt: *allowzero anyopaque, N: usize) [*c]u8;
        extern fn sqlite3_column_type(pStmt: *allowzero anyopaque, iCol: usize) usize;
        extern fn sqlite3_column_blob(pStmt: *allowzero anyopaque, iCol: usize) [*c]i8;
        extern fn sqlite3_column_double(pStmt: *allowzero anyopaque, iCol: usize) f64;
        extern fn sqlite3_column_int(pStmt: *allowzero anyopaque, iCol: usize) c_int;
        extern fn sqlite3_column_int64(pStmt: *allowzero anyopaque, iCol: usize) i64;
        extern fn sqlite3_column_text(pStmt: *allowzero anyopaque, iCol: usize) [*c]u8;
        extern fn sqlite3_column_bytes(pStmt: *allowzero anyopaque, iCol: usize) usize;
        extern fn sqlite3_bind_blob(pStmt: *allowzero anyopaque, index: usize, blob: [*c]const i8, len: usize, lifetime_opt: *allowzero anyopaque) usize;
        extern fn sqlite3_bind_blob64(pStmt: *allowzero anyopaque, index: usize, blob: [*c]const i8, len: usize, lifetime_opt: *allowzero anyopaque) usize;
        extern fn sqlite3_bind_double(pStmt: *allowzero anyopaque, index: usize, v: f64) usize;
        extern fn sqlite3_bind_int(pStmt: *allowzero anyopaque, index: usize, v: i32) usize;
        extern fn sqlite3_bind_int64(pStmt: *allowzero anyopaque, index: usize, v: i64) usize;
        extern fn sqlite3_bind_null(pStmt: *allowzero anyopaque, index: usize) usize;
        extern fn sqlite3_bind_text(pStmt: *allowzero anyopaque, index: usize, text: [*c]const u8, len: usize, lifetime_opt: *allowzero anyopaque) usize;
        extern fn sqlite3_bind_parameter_index(pStmt: *allowzero anyopaque, zName: [*c]const u8) usize;
    };
};

// tests

test "query" {
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
            \\row 0: 1=db.ValueType{ .INT64 = 1 },"hello"=db.ValueType{ .TEXT = { 104, 101, 108, 108, 111 } },5.0=db.ValueType{ .FLOAT64 = 5.0e+00 },
            \\
        ;
        output_buf.clearRetainingCapacity();
        var q = try Query.init(testing.allocator, &db, "select 1, \"hello\", 5.0", null);
        defer q.deinit();
        var rit = try q.iterator();
        while (true) {
            var maybe_row = try rit.next();
            if (maybe_row) |*row| {
                try output_writer.print("row {d}: ", .{row.row_seqid});
                var cit = row.iterator();
                while (try cit.next()) |col| {
                    defer col.deinit();
                    try output_writer.print("{s}={any},", .{ col.name, col.value });
                }
                try output_writer.print("\n", .{});
            } else {
                break;
            }
        }
        try testing.expectEqualSlices(u8, expected_output, output_buf.items);
    }
    {
        const expected_output =
            \\row 0: ?=db.ValueType{ .INT64 = 1 },$name=db.ValueType{ .FLOAT64 = 5.0e+00 },?2=db.ValueType{ .FLOAT64 = 5.0e+00 },
            \\
        ;
        output_buf.clearRetainingCapacity();
        var q = try Query.init(
            testing.allocator,
            &db,
            "select ?, $name, ?2",
            &[_]QueryArg{
                QueryArg{ .value = .{ .INT64 = 1 } },
                QueryArg{ .namez = "$name", .value = .{ .TEXT = "hello" } },
                QueryArg{ .value = .{ .FLOAT64 = 5.0 } },
            },
        );
        defer q.deinit();
        var rit = try q.iterator();
        while (true) {
            var maybe_row = try rit.next();
            if (maybe_row) |*row| {
                try output_writer.print("row {d}: ", .{row.row_seqid});
                var cit = row.iterator();
                while (try cit.next()) |col| {
                    defer col.deinit();
                    try output_writer.print("{s}={any},", .{ col.name, col.value });
                }
                try output_writer.print("\n", .{});
            } else {
                break;
            }
        }
        try testing.expectEqualSlices(u8, expected_output, output_buf.items);
    }
    {
        const Record = struct {
            col1: i64,
            name: []const u8,
            col3: f64,

            pub fn free(this: *const @This(), allocator: std.mem.Allocator) void {
                allocator.free(this.name);
            }
        };
        var record: Record = undefined;
        var q = try Query.init(testing.allocator, &db, "select 1, \"hello\", 5.0", null);
        defer q.deinit();
        var rit = try q.iterator();
        var maybe_row = try rit.next();
        if (maybe_row) |*row| {
            try QueryRow.into(row, Record, &record);
            defer record.free(testing.allocator);
            try testing.expectEqualDeep(Record{ .col1 = 1, .name = "hello", .col3 = 5.0 }, record);
        } else {
            return error.RowNotExist;
        }
    }
    {
        const Record = struct {
            count: i64,
        };
        var records: [5]Record = undefined;
        var q = try Query.init(testing.allocator, &db, "with recursive cnt(x) as (select 1 union all select x+1 from cnt limit 5) select x from cnt", null);
        defer q.deinit();
        try q.into(Record, &records);
        try testing.expectEqualDeep([5]Record{
            Record{ .count = 1 },
            Record{ .count = 2 },
            Record{ .count = 3 },
            Record{ .count = 4 },
            Record{ .count = 5 },
        }, records);
    }
    {
        const Record = struct {
            count: i64,
        };
        var q = try Query.init(testing.allocator, &db, "with recursive cnt(x) as (select 1 union all select x+1 from cnt limit 5) select x from cnt", null);
        defer q.deinit();
        const records = try q.intoAlloc(Record, .{});
        defer q.allocator.free(records);
        try testing.expectEqualDeep(&[5]Record{
            Record{ .count = 1 },
            Record{ .count = 2 },
            Record{ .count = 3 },
            Record{ .count = 4 },
            Record{ .count = 5 },
        }, records);
    }
}

test "migration_funcs" {
    {
        var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
        defer sdb.deinit();
        var db: Db = sdb.getDb();
        try db.open();
        defer db.close();
        try testing.expectEqual(0, try db.queryExecuteSlice(testing.allocator, "create table 'mytab' ('id' INTEGER);"));
        try testing.expectEqual(true, try db.hasTable("mytab"));
    }
}

test "simple_migration" {
    const User = struct {
        id: i64,
        name: []const u8,
        sex: bool,
        hobby: ?[]const u8,
    };

    {
        var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
        defer sdb.deinit();
        var db: Db = sdb.getDb();
        try db.open();
        defer db.close();
        const MyMigration = struct {
            const Self = @This();
            allocator: std.mem.Allocator,
            db: *Db,

            pub fn getMigration(this: *Self) Migration {
                return Migration{
                    .id = "1",
                    .ctx = this,
                    .vtable = .{
                        .up = up,
                        .down = down,
                    },
                };
            }

            // vtable fns
            fn up(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const user_table = SchemaUtil.genSchemaOne(User);
                try m.db.createTable(&change_set, user_table, .{});
                return change_set;
            }

            fn down(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const user_table = SchemaUtil.genSchemaOne(User);
                try m.db.dropTable(&change_set, user_table, .{});
                return change_set;
            }
        };

        var my_migration = MyMigration{ .allocator = testing.allocator, .db = &db };
        var migration = my_migration.getMigration();
        try db.migrateUp(&migration);
        try db.migrateDown(&migration);
    }

    {
        var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
        defer sdb.deinit();
        var db: Db = sdb.getDb();
        try db.open();
        defer db.close();
        const MyMigration = struct {
            const Self = @This();
            allocator: std.mem.Allocator,
            db: *Db,

            pub fn getMigration(this: *Self) Migration {
                return Migration{
                    .id = "1",
                    .ctx = this,
                    .vtable = .{
                        .up = up,
                    },
                };
            }

            // vtable fns
            fn up(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const user_table = SchemaUtil.genSchemaOne(User);
                try m.db.createTable(&change_set, user_table, .{});
                try m.db.addColumn(&change_set, user_table.table_name, "nickname", ValueType.getUndefined([]const u8), .{});
                return change_set;
            }
        };

        var my_migration = MyMigration{ .allocator = testing.allocator, .db = &db };
        var migration = my_migration.getMigration();
        try db.migrateUp(&migration);
    }

    {
        var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
        defer sdb.deinit();
        var db: Db = sdb.getDb();
        try db.open();
        defer db.close();
        const MyMigration = struct {
            const Self = @This();
            allocator: std.mem.Allocator,
            db: *Db,

            pub fn getMigration(this: *Self) Migration {
                return Migration{
                    .id = "1",
                    .ctx = this,
                    .vtable = .{
                        .up = up,
                    },
                };
            }

            // vtable fns
            fn up(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const user_table = SchemaUtil.genSchemaOne(User);
                try m.db.createTable(&change_set, user_table, .{});
                try m.db.dropColumn(&change_set, user_table.table_name, "name", .{});
                return change_set;
            }
        };

        var my_migration = MyMigration{ .allocator = testing.allocator, .db = &db };
        var migration = my_migration.getMigration();
        try db.migrateUp(&migration);
    }

    {
        var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
        defer sdb.deinit();
        var db: Db = sdb.getDb();
        try db.open();
        defer db.close();
        const MyMigration = struct {
            const Self = @This();
            allocator: std.mem.Allocator,
            db: *Db,

            pub fn getMigration(this: *Self) Migration {
                return Migration{
                    .id = "1",
                    .ctx = this,
                    .vtable = .{
                        .up = up,
                    },
                };
            }

            // vtable fns
            fn up(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const user_table = SchemaUtil.genSchemaOne(User);
                try m.db.createTable(&change_set, user_table, .{});
                try m.db.renameTable(&change_set, user_table.table_name, "user_renamed", .{});
                return change_set;
            }
        };

        var my_migration = MyMigration{ .allocator = testing.allocator, .db = &db };
        var migration = my_migration.getMigration();
        try db.migrateUp(&migration);
    }

    {
        var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
        defer sdb.deinit();
        var db: Db = sdb.getDb();
        try db.open();
        defer db.close();
        const MyMigration = struct {
            const Self = @This();
            allocator: std.mem.Allocator,
            db: *Db,

            pub fn getMigration(this: *Self) Migration {
                return Migration{
                    .id = "1",
                    .ctx = this,
                    .vtable = .{
                        .up = up,
                    },
                };
            }

            // vtable fns
            fn up(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const user_table = SchemaUtil.genSchemaOne(User);
                try m.db.createTable(&change_set, user_table, .{});
                try m.db.renameColumn(&change_set, user_table.table_name, "name", "nickname", .{});
                return change_set;
            }
        };

        var my_migration = MyMigration{ .allocator = testing.allocator, .db = &db };
        var migration = my_migration.getMigration();
        try db.migrateUp(&migration);
    }
}

test "complex_migration" {
    const Company = struct {
        id: i64,
        name: []const u8,
        pub const indexes = .{
            .{ [_][]const u8{"name"}, IndexOpts{ .unique = true, .sortings = &[1]IndexOpts.Sorting{.desc} } },
        };
    };
    const Skill = struct {
        id: i64,
        name: []const u8,
    };
    const User = struct {
        id: i64,
        name: []const u8,
        sex: bool,
        hobby: ?[]const u8,
        company_id: i64,
        skill_id: i64,

        // has to be pub as if not zig may not compile it
        pub const primary_key = [_][]const u8{"id"};
        pub const col_opts = .{
            .{ "company_id", ColOpts{ .unique = true } },
        };
        pub const relations = .{
            .{ "company_id", Company, "id", RelationOpts{ .cardinality = .one_to_one } },
            .{ "skill_id", Skill, "id", RelationOpts{ .cardinality = .many_to_many } },
        };
    };
    {
        var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
        defer sdb.deinit();
        var db: Db = sdb.getDb();
        try db.open();
        defer db.close();
        const MyMigration = struct {
            const Self = @This();
            allocator: std.mem.Allocator,
            db: *Db,

            pub fn getMigration(this: *Self) Migration {
                return Migration{
                    .id = "1",
                    .ctx = this,
                    .vtable = .{
                        .up = up,
                    },
                };
            }

            // vtable fns
            fn up(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const table_schemas = SchemaUtil.genSchema(.{ User, Company, Skill });
                try m.db.createTables(&change_set, table_schemas, .{});
                return change_set;
            }
        };

        var my_migration = MyMigration{ .allocator = testing.allocator, .db = &db };
        var migration = my_migration.getMigration();
        try db.migrateUp(&migration);

        const MyMigration2 = struct {
            const Self = @This();
            allocator: std.mem.Allocator,
            db: *Db,

            pub fn getMigration(this: *Self) Migration {
                return Migration{
                    .id = "2",
                    .ctx = this,
                    .vtable = .{
                        .up = up,
                    },
                };
            }

            // vtable fns
            fn up(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const user_table_schema = SchemaUtil.genSchemaOne(User);
                const new_col_type = user_table_schema.columns[3].type;
                var new_col_opts = user_table_schema.columns[3].opts;
                new_col_opts.nullable = false;
                new_col_opts.default_value = ValueType{ .TEXT = "woodworking" };
                try m.db.alterColumn(&change_set, user_table_schema.table_name, "hobby", new_col_type, new_col_opts, .{});
                return change_set;
            }
        };

        var my_migration2 = MyMigration2{ .allocator = testing.allocator, .db = &db };
        var migration2 = my_migration2.getMigration();
        try db.migrateUp(&migration2);
    }
}

test "validate_table_schemas" {
    const Company = struct {
        id: i64,
        name: []const u8,
        pub const indexes = .{
            .{ [_][]const u8{"name"}, IndexOpts{ .unique = true, .sortings = &[1]IndexOpts.Sorting{.desc} } },
        };
    };
    const Skill = struct {
        id: i64,
        name: []const u8,
    };
    const User = struct {
        id: i64,
        name: []const u8,
        sex: bool,
        hobby: ?[]const u8,
        company_id: i64,
        skill_id: i64,

        // has to be pub as if not zig may not compile it
        pub const primary_key = [_][]const u8{"id"};
        pub const col_opts = .{
            .{ "company_id", ColOpts{ .unique = true } },
        };
        pub const relations = .{
            .{ "company_id", Company, "id", RelationOpts{ .cardinality = .one_to_one } },
            .{ "skill_id", Skill, "id", RelationOpts{ .cardinality = .many_to_many } },
        };
    };
    const TestUtil = struct {
        var buf = std.ArrayList([]const u8).init(testing.allocator);

        pub fn strMapKeys(m: anytype) []const []const u8 {
            buf.clearRetainingCapacity();
            var it = m.keyIterator();
            while (it.next()) |kptr| {
                buf.append(kptr.*) catch unreachable;
            }
            return buf.items;
        }
    };
    defer TestUtil.buf.deinit();
    {
        var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
        defer sdb.deinit();
        var db: Db = sdb.getDb();
        try db.open();
        defer db.close();
        const table_schemas = SchemaUtil.genSchema(.{ User, Company, Skill });
        var table_availability = try db.validateTableSchemas(testing.allocator, table_schemas);
        defer {
            table_availability.deinit();
        }
        try testing.expectEqual(4, table_availability.map.count());

        const user_entry = if (table_availability.map.get("User")) |u| u else unreachable;
        try testing.expectEqualDeep(&[_][]const u8{"Company"}, TestUtil.strMapKeys(user_entry.to_create.dependency_map));

        const company_entry = if (table_availability.map.get("Company")) |c| c else unreachable;
        try testing.expectEqualDeep(&[_][]const u8{}, TestUtil.strMapKeys(company_entry.to_create.dependency_map));

        const skill_entry = if (table_availability.map.get("Skill")) |c| c else unreachable;
        try testing.expectEqualDeep(&[_][]const u8{}, TestUtil.strMapKeys(skill_entry.to_create.dependency_map));

        const m2m_entry = if (table_availability.map.get("m2m_Skill_id_User_skill_id")) |m| m else unreachable;
        try testing.expectEqualDeep(&[_][]const u8{ "User", "Skill" }, TestUtil.strMapKeys(m2m_entry.to_create.dependency_map));
    }
}

test "index_funcs" {
    const Company = struct {
        id: i64,
        name: []const u8,
        location: ?[]const u8,
        pub const indexes = .{
            .{ [_][]const u8{"name"}, IndexOpts{ .unique = true, .sortings = &[1]IndexOpts.Sorting{.desc} } },
        };
    };
    {
        var sdb = try SqliteDb.init(testing.allocator, ":memory:", .{});
        defer sdb.deinit();
        var db: Db = sdb.getDb();
        try db.open();
        defer db.close();
        const MyMigration = struct {
            const Self = @This();
            allocator: std.mem.Allocator,
            db: *Db,

            pub fn getMigration(this: *Self) Migration {
                return Migration{
                    .id = "1",
                    .ctx = this,
                    .vtable = .{
                        .up = up,
                        .down = down,
                    },
                };
            }

            // vtable fns
            fn up(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const company_table = SchemaUtil.genSchemaOne(Company);
                try m.db.createTable(&change_set, company_table, .{});
                try m.db.createIndex(
                    &change_set,
                    company_table.table_name,
                    TableSchema.Index{
                        .name = "idx_for_test",
                        .keys = &[_][]const u8{"location"},
                        .opts = .{},
                    },
                    .{},
                );
                return change_set;
            }

            fn down(ctx: *anyopaque) Error!?ChangeSet {
                const m: *Self = @ptrCast(@alignCast(ctx));
                var change_set = try ChangeSet.init(m.allocator);
                const company_table = comptime SchemaUtil.genSchemaOne(Company);
                const index_name = company_table.indexes[0].name;
                const new_index_name = index_name ++ "_new";
                // renameIndex will call dropIndex, so we tested dropIndex too
                try m.db.renameIndex(
                    &change_set,
                    index_name,
                    new_index_name,
                    .{},
                );
                return change_set;
            }
        };

        var my_migration = MyMigration{ .allocator = testing.allocator, .db = &db };
        var migration = my_migration.getMigration();
        // abuse migrateUp & migrateDown for testing
        try db.migrateUp(&migration);
        try testing.expectEqual(true, try db.hasIndex("idx_Company_name"));
        try testing.expectEqual(true, try db.hasIndex("idx_for_test"));
        try db.migrateDown(&migration);
        try testing.expectEqual(true, try db.hasIndex("idx_Company_name_new"));
    }
}
