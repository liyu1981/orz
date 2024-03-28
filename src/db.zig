const std = @import("std");

const verbose = true;

// utils

inline fn isTuple(comptime T: type) bool {
    switch (@typeInfo(T)) {
        .Struct => |s| return s.is_tuple,
        else => return false,
    }
}

inline fn isStruct(comptime T: type) bool {
    switch (@typeInfo(T)) {
        .Struct => |s| return !s.is_tuple,
        else => return false,
    }
}

inline fn isStringLiteral(comptime T: type) bool {
    switch (@typeInfo(T)) {
        .Pointer => |p| {
            // TODO: is this enough?
            const cti = @typeInfo(p.child);
            switch (cti) {
                .Array => |a| {
                    return a.child == u8;
                },
                else => return false,
            }
        },
        else => return false,
    }
}

// general errors for db related fns

pub const Error = error{
    DbConnFailed,
    DbUseDatabaseFailed,
    StmtPrepareFailed,
    StmtFetchRowFailed,
    StmtFetchRowBusy,
    StmtFetchRowMisuse,
    StmtBindMisuse,
    StmtBindNameNotFound,
    StmtBindTooBig,
    StmtBindNomem,
    StmtBindRange,
} || std.mem.Allocator.Error;

// data type

pub const DataType = enum {
    NULL,
    BOOL,
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

// schema & create

pub const TableSchema = struct {
    table_name: []const u8 = undefined,
    col_names: [][]const u8 = undefined,
    col_types: []ValueType = undefined,
    primary_key_names: [][]const u8 = undefined,
    has_foreign_key: bool = false,
    foreign_key_names: [][]const u8 = undefined,
    foreign_key_target_table_names: [][]const u8 = undefined,
    foreign_key_target_col_names: [][]const u8 = undefined,
    foreign_key_relation_optss: []RelationOpts = undefined,
    has_indexes: bool = false,
    index_names: [][]const u8 = undefined,
    index_keys: [][]const u8 = undefined,
    index_optss: []IndexOpts = undefined,
};

pub const RelationOpts = struct {
    cardinality: enum { one_to_one, one_to_many } = .one_to_one,
    delete_action: enum { cascade, restrict, no_action, set_null, set_default } = .cascade,
    update_action: enum { cascade, restrict, no_action, set_null, set_default } = .cascade,
};

pub const IndexOpts = struct {};

pub const SchemaUtil = struct {
    fn checkEntDef(comptime EntType: type) void {
        const ti = @typeInfo(EntType);
        const vt = @typeInfo(ValueType).Union;
        switch (ti) {
            .Struct => |st| {
                if (st.is_tuple) {
                    @compileError("EntType must be struct, found tuple:" ++ @typeName(EntType));
                }
                inline for (st.fields) |field| {
                    comptime var found: bool = false;
                    inline for (vt.fields) |vtfield| {
                        if (vtfield.type == field.type) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        @compileError("EntType field '" ++ field.name ++ "'' is not allowed in ValueType, found:" ++ @typeName(field.type));
                    }
                }
            },
            else => {
                @compileError("EntType must be struct, found:" ++ @typeName(EntType));
            },
        }
    }

    fn getLastTypeName(comptime EntType: type) []const u8 {
        const full_type_name = @typeName(EntType);
        var it = std.mem.splitBackwardsAny(u8, full_type_name, ".");
        if (it.next()) |last_name| {
            return last_name;
        } else {
            @panic("Impossible, no last name fragment?:" ++ full_type_name);
        }
    }

    fn assertValidRelationTuple(table_name: []const u8, i: usize, decl_relation: anytype) void {
        comptime var buf: [1024]u8 = undefined;
        const str_i = std.fmt.bufPrintIntToSlice(&buf, i, 10, .lower, .{});
        if (!isStringLiteral(@TypeOf(decl_relation[0]))) {
            @compileError(table_name ++ " entry " ++ str_i ++ " with wrong type at item 0, expect string literal(*const [?:0]u8), found " ++ @typeName(@TypeOf(decl_relation[0])));
        }
        if (!isStruct(decl_relation[1])) {
            @compileError(table_name ++ " entry " ++ str_i ++ " with wrong type at item 1, expect Struct, found " ++ @typeName(decl_relation[1]));
        }
        if (!isStringLiteral(@TypeOf(decl_relation[2]))) {
            @compileError(table_name ++ " entry " ++ str_i ++ " with wrong type at item 2, expect string literal(*const [?:0]u8), found " ++ @typeName(@TypeOf(decl_relation[2])));
        }
        if (!isStruct(@TypeOf(decl_relation[3]))) {
            @compileError(table_name ++ " entry " ++ str_i ++ " with wrong type at item 3, expect Struct, found " ++ @typeName(decl_relation[3]));
        }
    }

    fn calcForeignKeyName(table_name: []const u8, i: usize, decl_relation: anytype) []const u8 {
        if (!isTuple(@TypeOf(decl_relation))) {
            @compileError("each entry in relations must be tuple, fund:" ++ @typeName(@TypeOf(decl_relation)));
        }
        assertValidRelationTuple(table_name, i, decl_relation);
        const target_table_name = getLastTypeName(decl_relation[1]);
        return "fk_" ++ table_name ++ "_" ++ target_table_name ++ "_" ++ decl_relation[2];
    }

    fn calcForeignKeyArrays(
        comptime table_name: []const u8,
        comptime decl_relations: anytype,
        comptime fk_names: [][]const u8,
        comptime fk_target_table_names: [][]const u8,
        comptime fk_target_col_names: [][]const u8,
        comptime fk_relation_optss: []RelationOpts,
    ) void {
        if (!isTuple(@TypeOf(decl_relations))) {
            @compileError("relations must be tuple, fund:" ++ @typeName(@TypeOf(decl_relations)));
        }
        inline for (0..decl_relations.len) |i| {
            fk_names[i] = calcForeignKeyName(table_name, i, decl_relations[i]);
            fk_target_table_names[i] = getLastTypeName(decl_relations[i][1]);
            fk_target_col_names[i] = decl_relations[i][2];
            fk_relation_optss[i] = decl_relations[i][3];
        }
    }

    fn calcIndexName(comptime table_name: []const u8, comptime i: usize, comptime decl_index: anytype) []const u8 {
        _ = i;
        if (!isTuple(@TypeOf(decl_index))) {
            @compileError("each entry in indexes must be tuple, fund:" ++ @typeName(@TypeOf(decl_index)));
        }
        comptime var final_name: []const u8 = "idx_" ++ table_name;
        inline for (0..decl_index[0].len) |j| {
            final_name = final_name ++ "_" ++ decl_index[0][j];
        }
        return final_name;
    }

    fn calcIndexKey(comptime decl_index: anytype) []const u8 {
        if (!isTuple(@TypeOf(decl_index))) {
            @compileError("each entry in indexes must be tuple, fund:" ++ @typeName(@TypeOf(decl_index)));
        }
        comptime var key: []const u8 = "";
        inline for (0..decl_index[0].len) |j| {
            if (j == 0) {
                key = key ++ decl_index[0][j];
            } else {
                key = key ++ "," ++ decl_index[0][j];
            }
        }
        return key;
    }

    fn calcIndexArrays(
        comptime table_name: []const u8,
        comptime decl_indexes: anytype,
        comptime index_names: [][]const u8,
        comptime index_keys: [][]const u8,
        comptime index_optss: []IndexOpts,
    ) void {
        if (!isTuple(@TypeOf(decl_indexes))) {
            @compileError("indexes must be tuple, fund:" ++ @typeName(@TypeOf(decl_indexes)));
        }
        inline for (0..decl_indexes.len) |i| {
            index_names[i] = calcIndexName(table_name, i, decl_indexes[i]);
            index_keys[i] = calcIndexKey(decl_indexes[i]);
            index_optss[i] = decl_indexes[i][1];
        }
    }

    pub fn calcTableSchema(comptime EntType: type) TableSchema {
        const ti_struct = @typeInfo(EntType).Struct;
        const fields = ti_struct.fields;
        const decls = ti_struct.decls;

        const table_name = getLastTypeName(EntType);

        comptime var ent_col_names: [fields.len][]const u8 = undefined;
        comptime var ent_col_types: [fields.len]ValueType = undefined;
        inline for (0..fields.len) |i| {
            ent_col_names[i] = fields[i].name;
            ent_col_types[i] = comptime ValueType.getUndefined(fields[i].type);
        }

        comptime var with_decl_primary_key: bool = false;
        comptime var with_decl_relations: bool = false;
        comptime var with_decl_indexes: bool = false;
        comptime {
            for (0..decls.len) |i| {
                if (std.mem.eql(u8, decls[i].name, "relations")) {
                    with_decl_relations = true;
                }
                if (std.mem.eql(u8, decls[i].name, "primary_key")) {
                    with_decl_primary_key = true;
                }
                if (std.mem.eql(u8, decls[i].name, "indexes")) {
                    with_decl_indexes = true;
                }
            }
        }

        comptime var primary_key_names: [if (with_decl_primary_key) @field(EntType, "primary_key").len else 1][]const u8 = undefined;
        comptime {
            if (with_decl_primary_key) {
                const decl_primary_key = @field(EntType, "primary_key");
                for (0..decl_primary_key.len) |i| {
                    primary_key_names[i] = decl_primary_key[i];
                }
            } else {
                // first col will be used as primary key
                if (ent_col_names.len == 0) {
                    @compileError(@typeName(EntType) ++ " has no field can be used as primary key!");
                }
                primary_key_names[0] = ent_col_names[0];
            }
        }

        const has_foreign_key: bool = if (with_decl_relations) @field(EntType, "relations").len > 0 else false;
        comptime var foreign_key_names: [if (with_decl_relations) @field(EntType, "relations").len else 0][]const u8 = undefined;
        comptime var foreign_key_target_table_names: [if (with_decl_relations) @field(EntType, "relations").len else 0][]const u8 = undefined;
        comptime var foreign_key_target_col_names: [if (with_decl_relations) @field(EntType, "relations").len else 0][]const u8 = undefined;
        comptime var foreign_key_relation_optss: [if (with_decl_relations) @field(EntType, "relations").len else 0]RelationOpts = undefined;
        comptime {
            if (has_foreign_key) {
                calcForeignKeyArrays(
                    table_name,
                    @field(EntType, "relations"),
                    &foreign_key_names,
                    &foreign_key_target_table_names,
                    &foreign_key_target_col_names,
                    &foreign_key_relation_optss,
                );
            }
        }

        const has_indexes = if (with_decl_indexes) @field(EntType, "indexes").len > 0 else false;
        comptime var index_names: [if (with_decl_indexes) @field(EntType, "indexes").len else 0][]const u8 = undefined;
        comptime var index_keys: [if (with_decl_indexes) @field(EntType, "indexes").len else 0][]const u8 = undefined;
        comptime var index_optss: [if (with_decl_indexes) @field(EntType, "indexes").len else 0]IndexOpts = undefined;
        comptime {
            if (has_indexes) {
                calcIndexArrays(
                    table_name,
                    @field(EntType, "indexes"),
                    &index_names,
                    &index_keys,
                    &index_optss,
                );
            }
        }

        return TableSchema{
            .table_name = table_name,
            .col_names = &ent_col_names,
            .col_types = &ent_col_types,
            .primary_key_names = &primary_key_names,
            .has_foreign_key = has_foreign_key,
            .foreign_key_names = &foreign_key_names,
            .foreign_key_target_table_names = &foreign_key_target_table_names,
            .foreign_key_target_col_names = &foreign_key_target_col_names,
            .foreign_key_relation_optss = &foreign_key_relation_optss,
            .has_indexes = has_indexes,
            .index_names = &index_names,
            .index_keys = &index_keys,
            .index_optss = &index_optss,
        };
    }

    pub fn buildOne(comptime EntType: type) TableSchema {
        checkEntDef(EntType);
        return comptime calcTableSchema(EntType);
    }

    pub fn build(comptime ent_type_tuple: anytype) [ent_type_tuple.len]TableSchema {
        if (!isTuple(@TypeOf(ent_type_tuple))) {
            @compileError("build only accepts a tuple of ent type struct, found:" ++ @typeName(@TypeOf(ent_type_tuple)));
        }
        var table_schemas: [ent_type_tuple.len]TableSchema = undefined;
        inline for (0..ent_type_tuple.len) |i| {
            table_schemas[i] = SchemaUtil.buildOne(ent_type_tuple[i]);
        }
        return table_schemas;
    }
};

// constraint

pub const Constraint = struct {};

// query

pub const ValueType = union(DataType) {
    NULL: void,
    BOOL: bool,
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
    BLOB: []const i8,

    pub fn getUndefined(comptime T: type) ValueType {
        switch (T) {
            void => return ValueType{ .NULL = undefined },
            bool => return ValueType{ .BOOL = undefined },
            i8 => return ValueType{ .INT8 = undefined },
            u8 => return ValueType{ .UINT8 = undefined },
            i16 => return ValueType{ .INT16 = undefined },
            u16 => return ValueType{ .UINT16 = undefined },
            i32 => return ValueType{ .INT32 = undefined },
            u32 => return ValueType{ .UINT32 = undefined },
            i64 => return ValueType{ .INT64 = undefined },
            u64 => return ValueType{ .UINT64 = undefined },
            f32 => return ValueType{ .FLOAT32 = undefined },
            f64 => return ValueType{ .FLOAT64 = undefined },
            []const u8 => return ValueType{ .TEXT = undefined },
            []const i8 => return ValueType{ .BLOB = undefined },
            else => {
                @compileError("Unsupported type: " ++ @typeName(T));
            },
        }
    }

    pub fn free(this: ValueType, allocator: std.mem.Allocator) void {
        switch (this) {
            DataType.TEXT => allocator.free(this.TEXT),
            DataType.BLOB => allocator.free(this.BLOB),
            else => {},
        }
    }

    pub fn getValue(vt: *const ValueType, comptime WantedType: type, allocator: std.mem.Allocator) !WantedType {
        const ti = @typeInfo(ValueType);
        inline for (ti.Union.fields) |field| {
            if (field.type == WantedType) {
                switch (field.type) {
                    []const u8 => return try allocator.dupe(u8, @field(vt, field.name)),
                    []const i8 => return try allocator.dupe(i8, @field(vt, field.name)),
                    else => return @field(vt, field.name),
                }
            }
        }
        @panic(@typeName(WantedType) ++ " can not be found in ValueType.");
    }
};

pub const QueryArg = struct {
    namez: ?[:0]const u8 = null,
    value: ValueType,
};

pub const Query = struct {
    pub const Iterator = struct {
        q: *Query,
        fetched_row_count: usize = 0,

        pub fn next(this: *Iterator) !?QueryRow {
            if (try this.q.db.queryNextRow()) |row_seqid| {
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
    raw_bind_args: ?[]const QueryArg = null,
    prepared: bool = false,
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
        if (!this.prepared) {
            try this.db.queryEvaluate(this);
        }
        return Iterator{ .q = this };
    }

    pub fn finalize(this: *Query) void {
        if (!this.finalized and this.prepared) {
            _ = this.db.queryFinalize(this);
        }
    }

    pub fn into(this: *Query, comptime DestType: type, dest_slice: []DestType) !void {
        var rit = try this.iterator();
        var i: usize = 0;
        while (true) {
            var maybe_row = try rit.next();
            if (maybe_row) |*row| {
                if (i >= dest_slice.len) {
                    return error.QueryIntoOutOfBounds;
                }
                try QueryRow.into(row, DestType, &dest_slice[i]);
                i += 1;
            } else break;
        }
    }

    pub fn intoAlloc(this: *Query, comptime DestType: type, opts: struct {
        alloc_batch_size: usize = 8,
    }) ![]DestType {
        var arr = try this.allocator.alloc(DestType, opts.alloc_batch_size);
        defer this.allocator.free(arr);
        var rit = try this.iterator();
        var i: usize = 0;
        while (true) {
            var maybe_row = try rit.next();
            if (maybe_row) |*row| {
                if (i >= arr.len) {
                    arr = try this.allocator.realloc(arr, arr.len * 2);
                }
                try QueryRow.into(row, DestType, &arr[i]);
                i += 1;
            } else break;
        }
        return try this.allocator.dupe(DestType, arr[0..i]);
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

    pub fn getColAtIdx(this: *QueryRow, idx: usize) !?QueryCol {
        const maybe_colpair = try this.q.db.rawQueryNextCol(this, idx);
        if (maybe_colpair) |colpair| {
            return QueryCol{
                .allocator = colpair.allocator,
                .row = this,
                .name = colpair.name,
                .value = colpair.value,
            };
        } else {
            return null;
        }
    }

    pub fn into(row: *QueryRow, DestType: type, dest: *DestType) !void {
        const dest_typeinfo = @typeInfo(DestType);
        switch (dest_typeinfo) {
            .Struct => {},
            else => {
                @compileError("dest_struct should be mutable pointer to struct, find pointer to:" ++ @typeName(DestType));
            },
        }
        if (row.q.col_count != dest_typeinfo.Struct.fields.len) {
            return error.RowIntoNoEqlStruct;
        }
        const dest_fields = dest_typeinfo.Struct.fields;
        inline for (dest_fields, 0..) |field, i| {
            const maybe_qc = try row.getColAtIdx(i);
            if (maybe_qc) |qc| {
                defer qc.deinit();
                @field(dest, field.name) = try ValueType.getValue(&qc.value, field.type, row.q.allocator);
            } else {
                switch (@typeInfo(field.type)) {
                    .Optional => {
                        @field(dest, field.name) = null;
                    },
                    else => {
                        std.debug.print("row col {d} is null while {s}: {s} accepts no null.\n", .{ i, field.name, @typeName(field.type) });
                        return error.RowColIsNull;
                    },
                }
            }
        }
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

// change set

pub const ChangeSet = struct {
    allocator: std.mem.Allocator,
    query_buf: []Query,
    query_len: usize = 0,

    pub fn init(allocator: std.mem.Allocator) !ChangeSet {
        return ChangeSet{
            .allocator = allocator,
            .query_buf = try allocator.alloc(Query, 8),
        };
    }

    pub fn deinit(this: *const ChangeSet) void {
        for (0..this.query_len) |i| {
            this.query_buf[i].deinit();
        }
        this.allocator.free(this.query_buf);
    }

    fn ensureCapcityFor(this: *ChangeSet, size: usize) !void {
        if (size + this.query_len >= this.query_buf.len) {
            this.query_buf = try this.allocator.realloc(this.query_buf, this.query_buf.len * 2);
        }
    }

    pub fn append(this: *ChangeSet, q: Query) !void {
        try this.ensureCapcityFor(1);
        this.query_buf[this.query_len] = q;
        this.query_len += 1;
    }
};

// migration

pub const Migration = struct {
    pub const VTable = struct {
        up: ?*const fn (ctx: *anyopaque) Error!?ChangeSet = null,
        down: ?*const fn (ctx: *anyopaque) Error!?ChangeSet = null,
    };

    id: []const u8,
    ctx: *anyopaque,
    vtable: VTable,

    pub inline fn up(this: *Migration) Error!?ChangeSet {
        if (this.vtable.up) |upfn| {
            return upfn(this.ctx);
        } else return null;
    }

    pub inline fn down(this: *Migration) Error!?ChangeSet {
        if (this.vtable.down) |downfn| {
            return downfn(this.ctx);
        } else return null;
    }
};

// db

pub const DbVTable = struct {
    pub const CreateDatabaseOpts = struct {
        auto_use: bool = true,
        error_if_exist: bool = true,
    };
    pub const MigrateDirection = enum { up, down };
    pub const CreateTableOpts = struct {};
    pub const DropTableOpts = struct {};
    pub const RenameTableOpts = struct {};
    pub const AddColumnOpts = struct {};
    pub const DropColumnOpts = struct {};
    pub const RenameColumnOpts = struct {};
    pub const CreateConstraintOpts = struct {};
    pub const DropConstraintOpts = struct {};

    implOpen: *const fn (ctx: *anyopaque) Error!void,
    implClose: *const fn (ctx: *anyopaque) void,

    implCreateDatabase: *const fn (ctx: *anyopaque, name: []const u8, opts: CreateDatabaseOpts) Error!void,
    implUseDatabase: *const fn (ctx: *anyopaque, name: []const u8) Error!void,

    implQueryEvaluate: *const fn (ctx: *anyopaque, query: *Query) Error!void,
    implQueryNextRow: *const fn (ctx: *anyopaque) Error!?usize,
    implQueryNextCol: *const fn (ctx: *anyopaque, row: *QueryRow, col_idx: usize) Error!?QueryCol,
    implQueryFinalize: *const fn (ctx: *anyopaque, query: *Query) usize,

    implMigrate: *const fn (ctx: *anyopaque, db: *Db, migration: *Migration, direction: MigrateDirection) Error!void,
    implCreateTable: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, col_names: [][]const u8, col_types: []ValueType, opts: CreateTableOpts) Error!void,
    implDropTable: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, opts: DropTableOpts) Error!void,
    implRenameTable: *const fn (ctx: *anyopaque, db: *Db, chagne_set: *ChangeSet, from_table_name: []const u8, to_table_name: []const u8, opts: RenameTableOpts) Error!void,
    implAddColumn: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, col_type: ValueType, opts: AddColumnOpts) Error!void,
    implDropColumn: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, opts: DropColumnOpts) Error!void,
    implRenameColumn: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, from_col_name: []const u8, to_col_name: []const u8, opts: RenameColumnOpts) Error!void,
    implCreateConstraint: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, constraint: Constraint, opts: CreateConstraintOpts) Error!void,
    implDropConstraint: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, constraint_name: []const u8, opts: DropConstraintOpts) Error!void,
};

// top level Db namespace starts here

ctx: *anyopaque,
vtable: DbVTable,
const Db = @This();

// vtable fn shortcuts

pub inline fn open(this: *Db) Error!void {
    try this.vtable.implOpen(this.ctx);
}

pub inline fn close(this: *Db) void {
    this.vtable.implClose(this.ctx);
}

pub inline fn createDatabase(this: *Db, name: []const u8, opts: DbVTable.CreateDatabaseOpts) Error!void {
    try this.vtable.implCreateDatabase(this.ctx, name, opts);
}

pub inline fn useDatabase(this: *Db, name: []const u8) Error!void {
    try this.vtable.implUseDatabase(this.ctx, name);
}

pub inline fn queryEvaluate(this: *Db, query: *Query) Error!void {
    if (verbose) {
        std.debug.print("evaluate query: {s}\n", .{query.raw_query});
    }
    try this.vtable.implQueryEvaluate(this.ctx, query);
}

pub inline fn queryNextRow(this: *Db) Error!?usize {
    return this.vtable.implQueryNextRow(this.ctx);
}

pub inline fn queryNextCol(this: *Db, row: *QueryRow, col_idx: usize) Error!?QueryCol {
    return this.vtable.implQueryNextCol(this.ctx, row, col_idx);
}

pub inline fn queryFinalize(this: *Db, query: *Query) usize {
    return this.vtable.implQueryFinalize(this.ctx, query);
}

pub inline fn migrateUp(this: *Db, migration: *Migration) Error!void {
    try this.vtable.implMigrate(this.ctx, this, migration, .up);
}

pub inline fn migrateDown(this: *Db, migration: *Migration) Error!void {
    try this.vtable.implMigrate(this.ctx, this, migration, .down);
}

// wrappers

pub fn queryExecute(this: *Db, query: *Query) Error!void {
    try this.queryEvaluate(query);
    var rit = try query.iterator();
    _ = try rit.next();
    query.finalize();
}

pub fn queryExecuteSlice(this: *Db, allocator: std.mem.Allocator, query: []const u8) Error!void {
    var q = try Query.init(allocator, this, query, null);
    try this.queryEvaluate(&q);
    var rit = try q.iterator();
    _ = try rit.next();
    q.finalize();
    q.deinit();
}

// migration fns

pub inline fn createTable(this: *Db, change_set: *ChangeSet, ent: TableSchema, opts: DbVTable.CreateTableOpts) Error!void {
    try this.vtable.implCreateTable(this.ctx, this, change_set, ent.table_name, ent.col_names, ent.col_types, opts);
}

pub inline fn dropTable(this: *Db, change_set: *ChangeSet, ent: TableSchema, opts: DbVTable.DropTableOpts) Error!void {
    try this.vtable.implDropTable(this.ctx, this, change_set, ent.table_name, opts);
}

pub inline fn renameTable(this: *Db, change_set: *ChangeSet, from_table_name: []const u8, to_table_name: []const u8, opts: DbVTable.RenameTableOpts) Error!void {
    try this.vtable.implRenameTable(this.ctx, this, change_set, from_table_name, to_table_name, opts);
}

pub inline fn addColumn(this: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, col_type: ValueType, opts: DbVTable.AddColumnOpts) Error!void {
    try this.vtable.implAddColumn(this.ctx, this, change_set, table_name, col_name, col_type, opts);
}

pub inline fn dropColumn(this: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, opts: DbVTable.DropColumnOpts) Error!void {
    try this.vtable.implDropColumn(this.ctx, this, change_set, table_name, col_name, opts);
}

pub inline fn renameColumn(this: *Db, change_set: *ChangeSet, table_name: []const u8, from_col_name: []const u8, to_col_name: []const u8, opts: DbVTable.RenameColumnOpts) Error!void {
    try this.vtable.implRenameColumn(this.ctx, this, change_set, table_name, from_col_name, to_col_name, opts);
}

pub inline fn createConstraint(this: *Db, change_set: *ChangeSet, table_name: []const u8, constraint: Constraint, opts: DbVTable.CreateConstraintOpts) Error!void {
    try this.vtable.implCreateConstraint(this.ctx, this, change_set, table_name, constraint, opts);
}

pub inline fn dropConstraint(this: *Db, change_set: *ChangeSet, table_name: []const u8, constraint_name: []const u8, opts: DbVTable.DropConstraintOpts) Error!void {
    try this.vtable.implDropConstraint(this.ctx, this, change_set, table_name, constraint_name, opts);
}
