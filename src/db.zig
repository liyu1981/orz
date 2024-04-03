const std = @import("std");
const testing = std.testing;

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

inline fn isArrayOf(comptime ChildType: type, comptime TestType: type) bool {
    switch (@typeInfo(TestType)) {
        .Array => |a| {
            return a.child == ChildType;
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
    RowIntoNoEqlStruct,
    RowColIsNull,
} || error{
    NoSpaceLeft,
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

pub const Order = enum { asc, desc };

pub const ColOpts = struct {
    nullable: bool = false,
    unique: bool = false,
    auto_increment: bool = false,
    default_value: ?ValueType = null,
    collate_method: ?[]const u8 = null,
};

pub const RelationOpts = struct {
    /// TODO: There is no one_to_many here, as essentially it can be defined from the other table with many_to_one. May be added later.
    association: enum { one_to_one, many_to_one, many_to_many } = .many_to_one,
    on_delete: Constraint.Rule = .cascade,
    on_update: Constraint.Rule = .cascade,
};

pub const IndexOpts = struct {
    unique: bool = false,
    order: ?[]const Order = null,
};

pub const ViewOpts = struct {
    pub const Schema = union(enum) {
        temp: void,
        name: []const u8,
    };
    schema: Schema = Schema{ .temp = {} },
    col_names: []const []const u8 = undefined,
};

pub const Constraint = union(enum) {
    pub const ConflictHandling = enum {
        rollback,
        abort,
        fail,
        ignore,
        replace,
    };
    pub const Rule = enum {
        set_null,
        set_default,
        cascade,
        restrict,
        no_action,
    };
    pub const Initially = enum {
        deferred,
        immediate,
    };
    pub const PrimaryKey = struct {
        name: ?[]const u8 = null,
        col_names: []const []const u8,
        on_conflict: ?ConflictHandling = null,
    };
    pub const Unique = struct {
        name: ?[]const u8 = null,
        col_names: []const []const u8,
        on_conflict: ?ConflictHandling = null,
    };
    pub const Check = struct {
        name: ?[]const u8 = null,
        expr: []const u8,
    };
    pub const Deferrable = struct {
        not: bool,
        initially: ?Initially = null,
    };
    pub const ForeignKey = struct {
        name: ?[]const u8 = null,
        col_names: []const []const u8,
        ref_table_name: []const u8,
        ref_table_col_names: []const []const u8,
        on_delete: ?Rule = null,
        on_update: ?Rule = null,
        match: ?[]const u8 = null,
        deferrable: ?Deferrable = null,
        opts: RelationOpts,
    };

    primary_key: PrimaryKey,
    unique: Unique,
    check: Check,
    foreign_key: ForeignKey,
};

pub const TableSchema = struct {
    pub const Column = struct {
        name: []const u8 = undefined,
        type: ValueType = undefined,
        opts: ColOpts = undefined,
    };
    pub const Index = struct {
        name: []const u8 = undefined,
        keys: []const []const u8 = undefined,
        opts: IndexOpts = undefined,
    };
    pub const Dependency = struct {
        table_name: []const u8 = undefined,
        satisfied: bool = false,
    };

    table_name: []const u8 = undefined,
    columns: []Column = undefined,
    primary_key: Constraint.PrimaryKey,
    foreign_keys: []Constraint.ForeignKey,
    indexes: []Index,
    checks: []Constraint.Check,
    dependencies: []Dependency = undefined,
};

pub const SchemaUtil = struct {
    fn checkEntDef(comptime EntType: type) void {
        comptime {
            const ti = @typeInfo(EntType);
            switch (ti) {
                .Struct => |st| {
                    if (st.is_tuple) {
                        @compileError("EntType must be struct, found tuple:" ++ @typeName(EntType));
                    }
                    for (st.fields) |field| {
                        if (!(ValueType.allow(field.type))) {
                            @compileError(@typeName(EntType) ++ " field '" ++ field.name ++ "' with type '" ++ @typeName(field.type) ++ "' is not allowed in " ++ @typeName(ValueType));
                        }
                    }
                },
                else => {
                    @compileError("EntType must be struct, found:" ++ @typeName(EntType));
                },
            }
        }
    }

    fn countEntTypeManyToManyRelation(comptime EntType: type) comptime_int {
        comptime {
            const ti_struct = @typeInfo(EntType).Struct;
            const decls = ti_struct.decls;
            var with_decl_relations: bool = false;
            for (0..decls.len) |i| {
                if (std.mem.eql(u8, decls[i].name, "relations")) {
                    with_decl_relations = true;
                }
            }
            return brk: {
                if (with_decl_relations) {
                    var count: comptime_int = 0;
                    const decl_relations = @field(EntType, "relations");
                    for (decl_relations) |decl_relation| {
                        if (decl_relation[3].association == .many_to_many) {
                            count += 1;
                        }
                    }
                    break :brk count;
                } else {
                    break :brk 0;
                }
            };
        }
    }

    /// will return the last segment of full name, e.g., sqlite3.Db.mytype => mytype.
    fn getLastTypeName(comptime EntType: type) []const u8 {
        const full_type_name = @typeName(EntType);
        var it = std.mem.splitBackwardsAny(u8, full_type_name, ".");
        if (it.next()) |last_name| {
            return last_name;
        } else {
            @panic("Impossible, no last name fragment?:" ++ full_type_name);
        }
    }

    fn toArrayOfStrings(comptime input: anytype) []const []const u8 {
        comptime {
            const ti = @typeInfo(@TypeOf(input));
            // @compileLog("first:", ti, "\n");
            switch (ti) {
                .Pointer => |pt| {
                    // @compileLog("second:", @typeInfo(pt.child), "\n");
                    // @compileLog(input);
                    if (pt.size == .Slice and pt.child == u8) {
                        return &[_][]const u8{input};
                    }
                    switch (@typeInfo(pt.child)) {
                        .Array => |at| {
                            if (at.child == u8) {
                                return &[_][]const u8{input};
                            }
                            const cti = @typeInfo(at.child);
                            if (cti == .Array and cti.Array.child == u8) {
                                return input;
                            }
                        },
                        else => {},
                    }
                },
                else => {},
            }
            @compileError("input is invalid!");
        }
    }

    fn assertValidRelationTuple(comptime table_name: []const u8, comptime i: usize, comptime decl_relation: anytype) void {
        comptime {
            var buf: [256]u8 = undefined;
            const str_i = std.fmt.bufPrintIntToSlice(&buf, i, 10, .lower, .{});
            if (decl_relation.len > 4) {
                var len_buf: [256]u8 = undefined;
                const str_len = std.fmt.bufPrintIntToSlice(&len_buf, decl_relation.len, 10, .lower, .{});
                @compileError(table_name ++ " entry " ++ str_i ++ " with number of items, expect tuple of 4 items, found " ++ str_len);
            }
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
    }

    fn assertValidIndexTuple(comptime table_name: []const u8, comptime i: usize, comptime decl_index: anytype) void {
        comptime {
            var buf: [1024]u8 = undefined;
            const str_i = std.fmt.bufPrintIntToSlice(&buf, i, 10, .lower, .{});
            if (decl_index.len > 2) {
                var len_buf: [256]u8 = undefined;
                const str_len = std.fmt.bufPrintIntToSlice(&len_buf, decl_index.len, 10, .lower, .{});
                @compileError(table_name ++ " entry " ++ str_i ++ " with number of items, expect tuple of 2 items, found " ++ str_len);
            }
            if (!isArrayOf([]const u8, @TypeOf(decl_index[0]))) {
                @compileError(table_name ++ " entry " ++ str_i ++ " with wrong type at item 0, expect [][]const u8, found " ++ @typeName(@TypeOf(decl_index[0])));
            }
            if (!isStruct(@TypeOf(decl_index[1]))) {
                @compileError(table_name ++ " entry " ++ str_i ++ " with wrong type at item 1, expect Struct, found " ++ @typeName(decl_index[1]));
            }
        }
    }

    fn calcForeignKeyName(
        comptime table_name: []const u8,
        comptime i: usize,
        comptime column_entry: TableSchema.Column,
        comptime decl_relation: anytype,
    ) []const u8 {
        comptime {
            if (!isTuple(@TypeOf(decl_relation))) {
                @compileError("each entry in relations must be tuple, fund:" ++ @typeName(@TypeOf(decl_relation)));
            }

            assertValidRelationTuple(table_name, i, decl_relation);

            const target_table_name = getLastTypeName(decl_relation[1]);

            switch (decl_relation[3].association) {
                .one_to_one => {
                    if (!column_entry.opts.unique) {
                        @compileError("table '" ++ table_name ++ "' col '" ++ decl_relation[2] ++ "' must be unique if relation to table '" ++ target_table_name ++ "' marked as .one_to_one!");
                    }
                },
                .many_to_one => {
                    if (column_entry.opts.unique) {
                        @compileError("table '" ++ table_name ++ "' col '" ++ decl_relation[2] ++ "' is unique while relation to table '" ++ target_table_name ++ "' marked as .many_to_one!");
                    }
                },
                .many_to_many => unreachable,
            }

            return "fk_" ++ table_name ++ "_" ++ target_table_name ++ "_" ++ decl_relation[2] ++ "_" ++
                switch (decl_relation[3].association) {
                .one_to_one => "o2o",
                .many_to_one => "m2o",
                .many_to_many => unreachable,
            };
        }
    }

    fn calcForeignKeyArrays(
        comptime table_name: []const u8,
        comptime columns: []TableSchema.Column,
        comptime decl_relations: anytype,
        comptime fks: []Constraint.ForeignKey,
    ) usize {
        comptime {
            if (!isTuple(@TypeOf(decl_relations))) {
                @compileError("relations must be tuple, fund:" ++ @typeName(@TypeOf(decl_relations)));
            }
            var fk_count: usize = 0;
            for (0..decl_relations.len) |i| {
                const decl_relation = decl_relations[i];
                switch (decl_relation[3].association) {
                    .one_to_one, .many_to_one => {
                        fks[i].name = calcForeignKeyName(
                            table_name,
                            i,
                            columns[i],
                            decl_relations[i],
                        );
                        fks[i].col_names = toArrayOfStrings(decl_relations[i][0]);
                        fks[i].ref_table_name = getLastTypeName(decl_relations[i][1]);
                        fks[i].ref_table_col_names = toArrayOfStrings(decl_relations[i][2]);
                        fks[i].opts = decl_relations[i][3];
                        fk_count += 1;
                    },
                    .many_to_many => {},
                }
            }
            return fk_count;
        }
    }

    fn calcIndexName(comptime table_name: []const u8, comptime i: usize, comptime decl_index: anytype) []const u8 {
        comptime {
            if (!isTuple(@TypeOf(decl_index))) {
                @compileError("each entry in indexes must be tuple, fund:" ++ @typeName(@TypeOf(decl_index)));
            }
            assertValidIndexTuple(table_name, i, decl_index);
            var final_name: []const u8 = "idx_" ++ table_name;
            for (0..decl_index[0].len) |j| {
                final_name = final_name ++ "_" ++ decl_index[0][j];
            }
            return final_name;
        }
    }

    /// will return a comma joined key array, like id,name
    fn calcIndexKey(comptime decl_index: anytype) []const []const u8 {
        comptime {
            if (!isTuple(@TypeOf(decl_index))) {
                @compileError("each entry in indexes must be tuple, fund:" ++ @typeName(@TypeOf(decl_index)));
            }
            var keys: [decl_index[0].len][]const u8 = undefined;
            for (0..decl_index[0].len) |j| {
                keys[j] = decl_index[0][j];
            }
            return &keys;
        }
    }

    fn calcIndexArrays(
        comptime table_name: []const u8,
        comptime decl_indexes: anytype,
        comptime indexes: []TableSchema.Index,
    ) void {
        comptime {
            if (!isTuple(@TypeOf(decl_indexes))) {
                @compileError("indexes must be tuple, fund:" ++ @typeName(@TypeOf(decl_indexes)));
            }
            for (0..decl_indexes.len) |i| {
                indexes[i].name = calcIndexName(table_name, i, decl_indexes[i]);
                indexes[i].keys = calcIndexKey(decl_indexes[i]);
                indexes[i].opts = decl_indexes[i][1];
            }
        }
    }

    fn calcColOpts(comptime T: type) ColOpts {
        switch (@typeInfo(T)) {
            .Optional => return ColOpts{ .nullable = true },
            else => return ColOpts{ .nullable = false },
        }
    }

    pub fn calcTableSchema(comptime EntType: type) TableSchema {
        comptime {
            const ti_struct = @typeInfo(EntType).Struct;
            const fields = ti_struct.fields;
            const decls = ti_struct.decls;

            var with_decl_primary_key: bool = false;
            var with_decl_relations: bool = false;
            var with_decl_indexes: bool = false;
            var with_decl_col_opts: bool = false;
            var with_decl_checks: bool = false;
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
                if (std.mem.eql(u8, decls[i].name, "col_opts")) {
                    with_decl_col_opts = true;
                }
                if (std.mem.eql(u8, decls[i].name, "checks")) {
                    with_decl_checks = true;
                }
            }

            const table_name = getLastTypeName(EntType);

            var ent_cols: [fields.len]TableSchema.Column = undefined;
            for (0..fields.len) |i| {
                ent_cols[i].name = fields[i].name;
                ent_cols[i].type = ValueType.getUndefined(fields[i].type);
                ent_cols[i].opts = calcColOpts(fields[i].type);
            }

            // col_opts need to be before others, especially foreign_keys
            if (with_decl_col_opts) {
                const decl_col_opts = @field(EntType, "col_opts");
                for (decl_col_opts) |entry| {
                    var col_idx: usize = 0;
                    var found_col_idx: bool = false;
                    for (0..ent_cols.len) |i| {
                        if (std.mem.eql(u8, ent_cols[i].name, entry[0])) {
                            found_col_idx = true;
                            col_idx = i;
                            break;
                        }
                    }
                    if (!found_col_idx) {
                        @compileError("invalid unique col name: not find '" ++ entry[0] ++ "' in " ++ @typeName(EntType));
                    }
                    ent_cols[col_idx].opts = entry[1];
                }
            }

            var primary_key: Constraint.PrimaryKey = undefined;
            if (with_decl_primary_key) {
                const decl_primary_key = @field(EntType, "primary_key");
                primary_key.col_names = &decl_primary_key;
                if (primary_key.col_names.len == 1) {
                    const pname_idx = brk: {
                        const pname = primary_key.col_names[0];
                        for (0..ent_cols.len) |i| {
                            if (std.mem.eql(u8, ent_cols[i].name, pname)) {
                                break :brk i;
                            }
                        }
                        unreachable;
                    };
                    ent_cols[pname_idx].opts.unique = true;
                }
            } else {
                // first col will be used as primary key
                if (ent_cols.len == 0) {
                    @compileError(@typeName(EntType) ++ " has no field can be used as primary key!");
                }
                primary_key.col_names = &[1][]const u8{ent_cols[0].name};
                ent_cols[0].opts.unique = true;
            }

            const has_foreign_key: bool = if (with_decl_relations) @field(EntType, "relations").len > 0 else false;
            const foreign_key_capacity = if (with_decl_relations) @field(EntType, "relations").len else 0;
            var foreign_keys: [foreign_key_capacity]Constraint.ForeignKey = undefined;
            const foreign_key_count = if (has_foreign_key)
                calcForeignKeyArrays(
                    table_name,
                    &ent_cols,
                    @field(EntType, "relations"),
                    &foreign_keys,
                )
            else
                @as(usize, 0);

            const has_indexes = if (with_decl_indexes) @field(EntType, "indexes").len > 0 else false;
            const index_capacity = if (with_decl_indexes) @field(EntType, "indexes").len else 0;
            var indexes: [index_capacity]TableSchema.Index = undefined;
            if (has_indexes) {
                calcIndexArrays(
                    table_name,
                    @field(EntType, "indexes"),
                    &indexes,
                );
            }

            const has_checks = if (with_decl_checks) @field(EntType, "checks").len > 0 else false;
            const checks_capacity = if (with_decl_checks) @field(EntType, "checks").len else 0;
            var checks: [checks_capacity]Constraint.Check = undefined;
            if (has_checks) {
                const decl_checks = @field(EntType, "checks");
                for (decl_checks, 0..) |decl_check, i| {
                    checks[i].expr = decl_check[0];
                }
            }

            return TableSchema{
                .table_name = table_name,
                .columns = &ent_cols,
                .primary_key = primary_key,
                .foreign_keys = foreign_keys[0..foreign_key_count],
                .indexes = &indexes,
                .checks = &checks,
            };
        }
    }

    /// check for base table whether they have valid decl of many2many relation etc. After checking, the checked flag will set to true.
    // TODO: what if there is already existing tables?
    pub fn checkRelation(comptime target: *TableSchema, comptime all_tables: []TableSchema) void {
        for (target.foreign_keys) |fk| {
            const found: bool = brk: {
                for (all_tables) |table| {
                    if (std.mem.eql(u8, fk.ref_table_name, table.table_name)) {
                        break :brk true;
                    }
                }
                break :brk false;
            };
            if (!found) {
                @compileError("'" ++ target.table_name ++ "'' specified a relation to not existing table '" ++ fk.table_name ++ "'");
            }
        }
    }

    inline fn calcM2MTableName(
        comptime table1_name: []const u8,
        comptime table1_col_name: []const u8,
        comptime table2_name: []const u8,
        comptime table2_col_name: []const u8,
    ) []const u8 {
        switch (std.mem.order(u8, table1_name, table2_name)) {
            .lt, .eq => return "m2m_" ++ table1_name ++ "_" ++ table1_col_name ++ "_" ++ table2_name ++ "_" ++ table2_col_name,
            .gt => return "m2m_" ++ table2_name ++ "_" ++ table2_col_name ++ "_" ++ table1_name ++ "_" ++ table1_col_name,
        }
    }

    fn genM2MTableSchema(
        comptime table_name: []const u8,
        comptime table1: *TableSchema,
        comptime table1_col_name: []const u8,
        comptime table2: *TableSchema,
        comptime table2_col_name: []const u8,
    ) TableSchema {
        comptime {
            const table1_col_type = brk: {
                for (0..table1.columns.len) |i| {
                    if (std.mem.eql(u8, table1_col_name, table1.columns[i].name)) {
                        break :brk table1.columns[i].type;
                    }
                }
                unreachable;
            };
            const table2_col_type = brk: {
                for (0..table2.columns.len) |i| {
                    if (std.mem.eql(u8, table2_col_name, table2.columns[i].name)) {
                        break :brk table2.columns[i].type;
                    }
                }
                unreachable;
            };

            const col1_name = table1.table_name ++ "_" ++ table1_col_name;
            const col2_name = table2.table_name ++ "_" ++ table2_col_name;

            var ent_cols = [_]TableSchema.Column{
                TableSchema.Column{
                    .name = "id",
                    .type = ValueType.getUndefined(i64),
                    .opts = ColOpts{},
                },
                TableSchema.Column{
                    .name = col1_name,
                    .type = table1_col_type,
                    .opts = ColOpts{},
                },
                TableSchema.Column{
                    .name = col2_name,
                    .type = table2_col_type,
                    .opts = ColOpts{},
                },
            };
            _ = &ent_cols;

            var foreign_keys = [_]Constraint.ForeignKey{
                Constraint.ForeignKey{
                    .name = "fk_" ++ table_name ++ "_0",
                    .col_names = toArrayOfStrings(col1_name),
                    .ref_table_name = table1.table_name,
                    .ref_table_col_names = toArrayOfStrings(table1_col_name),
                    .opts = RelationOpts{ .association = .many_to_one },
                },
                Constraint.ForeignKey{
                    .name = "fk_" ++ table_name ++ "_1",
                    .col_names = toArrayOfStrings(col2_name),
                    .ref_table_name = table2.table_name,
                    .ref_table_col_names = toArrayOfStrings(table2_col_name),
                    .opts = RelationOpts{ .association = .many_to_one },
                },
            };
            _ = &foreign_keys;

            const primary_key = Constraint.PrimaryKey{
                .col_names = &[_][]const u8{"id"},
            };
            ent_cols[0].opts.unique = true;

            return TableSchema{
                .table_name = table_name,
                .columns = &ent_cols,
                .primary_key = primary_key,
                .foreign_keys = &foreign_keys,
                .indexes = &[0]TableSchema.Index{},
                .checks = &[0]Constraint.Check{},
            };
        }
    }

    pub fn calcM2MTableSchema(
        comptime EntType: anytype,
        comptime target: *TableSchema,
        comptime all_tables: []TableSchema,
        comptime m2m_table_start: usize,
        comptime all_tables_len: usize,
    ) usize {
        var table_schema_added: usize = 0;
        var next_table_schema_slot: usize = all_tables_len;
        var total_tables_len: usize = all_tables_len;

        const ti_struct = @typeInfo(EntType).Struct;
        const decls = ti_struct.decls;

        comptime var with_decl_relations: bool = false;
        comptime {
            for (0..decls.len) |i| {
                if (std.mem.eql(u8, decls[i].name, "relations")) {
                    with_decl_relations = true;
                }
            }
        }

        if (!with_decl_relations) {
            return 0;
        }

        const decl_relations = @field(EntType, "relations");

        for (0..decl_relations.len) |i| {
            const decl_relation = decl_relations[i];
            switch (decl_relation[3].association) {
                .one_to_one, .many_to_one => {},
                .many_to_many => {
                    const target_col_name = decl_relation[0];
                    const target_foreign_col_name = decl_relation[2];
                    const target_foreign_table_name = getLastTypeName(decl_relation[1]);

                    const m2m_table_name = calcM2MTableName(
                        target.table_name,
                        target_col_name,
                        target_foreign_table_name,
                        target_foreign_col_name,
                    );
                    const found = brk: {
                        for (all_tables[m2m_table_start..total_tables_len]) |table| {
                            if (std.mem.eql(u8, m2m_table_name, table.table_name)) {
                                break :brk true;
                            }
                        }
                        break :brk false;
                    };
                    if (!found) {
                        const target_foreign = brk: {
                            const wanted = target_foreign_table_name;
                            for (0..m2m_table_start) |j| {
                                if (std.mem.eql(u8, wanted, all_tables[j].table_name)) {
                                    break :brk &all_tables[j];
                                }
                            }
                            unreachable;
                        };
                        all_tables[next_table_schema_slot] = genM2MTableSchema(
                            m2m_table_name,
                            target,
                            target_col_name,
                            target_foreign,
                            target_foreign_col_name,
                        );
                        next_table_schema_slot += 1;
                        total_tables_len += 1;
                        table_schema_added += 1;
                    } else {
                        continue;
                    }
                },
            }
        }

        return table_schema_added;
    }

    /// genSchemaOne will not do relation checking, which needs to be manually checked later
    pub fn genSchemaOne(comptime EntType: type) TableSchema {
        const final_table_schema = comptime brk: {
            checkEntDef(EntType);
            break :brk calcTableSchema(EntType);
        };
        return final_table_schema;
    }

    /// genSchema will auto do relation checking
    pub fn genSchema(comptime ent_type_tuple: anytype) []TableSchema {
        const final_table_schemas = comptime brk: {
            if (!isTuple(@TypeOf(ent_type_tuple))) {
                @compileError("build only accepts a tuple of ent type struct, found:" ++ @typeName(@TypeOf(ent_type_tuple)));
            }

            for (0..ent_type_tuple.len) |i| {
                checkEntDef(ent_type_tuple[i]);
            }

            var possible_m2m_table_count: usize = 0;
            for (0..ent_type_tuple.len) |i| {
                possible_m2m_table_count += countEntTypeManyToManyRelation(ent_type_tuple[i]);
            }

            var table_schemas: [ent_type_tuple.len + possible_m2m_table_count]TableSchema = undefined;
            var table_schema_gen_count: usize = 0;
            for (0..ent_type_tuple.len) |i| {
                table_schemas[i] = calcTableSchema(ent_type_tuple[i]);
                table_schema_gen_count += 1;
            }

            const based_table_schema_count = table_schema_gen_count;
            for (0..based_table_schema_count) |i| {
                const ent_type = ent_type_tuple[i];
                table_schema_gen_count += calcM2MTableSchema(
                    ent_type,
                    &table_schemas[i],
                    &table_schemas,
                    based_table_schema_count,
                    table_schema_gen_count,
                );
            }

            for (0..based_table_schema_count) |i| {
                SchemaUtil.checkRelation(&table_schemas[i], &table_schemas);
            }

            break :brk table_schemas[0..table_schema_gen_count];
        };
        return final_table_schemas;
    }

    pub inline fn dumpTableDependencies(table_schemas: []TableSchema) void {
        for (table_schemas) |table_schema| {
            std.debug.print("table {s} dependencies:", .{table_schema.table_name});
            if (table_schema.foreign_keys.len > 0) {
                for (table_schema.foreign_keys) |fk| {
                    std.debug.print("{s}, ", .{fk.ref_table_name});
                }
            }
            std.debug.print("\n", .{});
        }
    }
};

// query

pub const ValueType = union(DataType) {
    NULL: void, // not used but has to make zig compiler happy
    BOOL: bool,
    INT8: i8,
    UINT8: u8,
    INT16: i16,
    UINT16: u16,
    INT32: i32,
    UINT32: u32,
    INT64: i64,
    UINT64: u64,
    FLOAT32: struct { v: f32, precision: ?usize = null },
    FLOAT64: struct { v: f64, precision: ?usize = null },
    TEXT: []const u8,
    BLOB: []const i8,

    pub fn allow(comptime T: type) bool {
        switch (T) {
            bool, ?bool, i8, ?i8, u8, ?u8, i16, ?i16, u16, ?u16, i32, ?i32, u32, ?u32, i64, ?i64, u64, ?u64, f32, ?f32, f64, ?f64, []const u8, ?[]const u8, []const i8, ?[]const i8 => return true,
            else => return false,
        }
    }

    pub fn getUndefined(comptime T: type) ValueType {
        switch (T) {
            bool, ?bool => return ValueType{ .BOOL = undefined },
            i8, ?i8 => return ValueType{ .INT8 = undefined },
            u8, ?u8 => return ValueType{ .UINT8 = undefined },
            i16, ?i16 => return ValueType{ .INT16 = undefined },
            u16, ?u16 => return ValueType{ .UINT16 = undefined },
            i32, ?i32 => return ValueType{ .INT32 = undefined },
            u32, ?u32 => return ValueType{ .UINT32 = undefined },
            i64, ?i64 => return ValueType{ .INT64 = undefined },
            u64, ?u64 => return ValueType{ .UINT64 = undefined },
            f32, ?f32 => return ValueType{ .FLOAT32 = undefined },
            f64, ?f64 => return ValueType{ .FLOAT64 = undefined },
            []const u8, ?[]const u8 => return ValueType{ .TEXT = undefined },
            []const i8, ?[]const i8 => return ValueType{ .BLOB = undefined },
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

    pub fn toSql(this: *const ValueType, sql_writer: std.ArrayList(u8).Writer) !void {
        const FPrintUtil = struct {
            // morden long double requires at 21bytes to serialize, so we use 64 should be enough
            //https://en.cppreference.com/w/c/types/limits#Limits_of_floating_point_types
            var decimal_buf: [64]u8 = undefined;
            pub fn fToString(fv: anytype, precision: usize) !struct {
                integer: []const u8,
                fraction: []const u8,
            } {
                const intv = @round(fv * @as(@TypeOf(fv), @floatFromInt(std.math.pow(usize, 10, precision))));
                const vstr = try std.fmt.bufPrint(&decimal_buf, "{d}", .{intv});
                return .{
                    .integer = vstr[0 .. vstr.len - precision],
                    .fraction = vstr[(vstr.len - precision)..],
                };
            }
        };

        switch (this.*) {
            .BOOL => |bv| {
                if (bv) {
                    try sql_writer.print("TRUE", .{});
                } else {
                    try sql_writer.print("FALSE", .{});
                }
            },
            .INT8 => |i8v| try sql_writer.print("{d}", .{i8v}),
            .UINT8 => |u8v| try sql_writer.print("{d}", .{u8v}),
            .INT16 => |i16v| try sql_writer.print("{d}", .{i16v}),
            .UINT16 => |u16v| try sql_writer.print("{d}", .{u16v}),
            .INT32 => |i32v| try sql_writer.print("{d}", .{i32v}),
            .UINT32 => |u32v| try sql_writer.print("{d}", .{u32v}),
            .INT64 => |i64v| try sql_writer.print("{d}", .{i64v}),
            .UINT64 => |u64v| try sql_writer.print("{d}", .{u64v}),
            .FLOAT32 => |f32v| {
                if (f32v.precision) |p| {
                    const vstr = try FPrintUtil.fToString(f32v.v, p);
                    if (vstr.fraction.len > 0) {
                        try sql_writer.print("{s}.{s}", .{ vstr.integer, vstr.fraction });
                    } else {
                        try sql_writer.print("{s}", .{vstr.integer});
                    }
                } else {
                    try sql_writer.print("{d}", .{f32v.v});
                }
            },
            .FLOAT64 => |f64v| {
                if (f64v.precision) |p| {
                    const vstr = try FPrintUtil.fToString(f64v.v, p);
                    if (vstr.fraction.len > 0) {
                        try sql_writer.print("{s}.{s}", .{ vstr.integer, vstr.fraction });
                    } else {
                        try sql_writer.print("{s}", .{vstr.integer});
                    }
                } else {
                    try sql_writer.print("{d}", .{f64v.v});
                }
            },
            .TEXT => |tv| try sql_writer.print("'{s}'", .{tv}),
            .BLOB => {
                @panic("TODO blob toString");
            },
            else => unreachable,
        }
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

    /// when init, query will be allocator.dupeZ
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
                try QueryRow.into(row, &dest_slice[i]);
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
                try QueryRow.into(row, &arr[i]);
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
            const maybe_colpair = try this.row.q.db.queryNextCol(this.row, to_fetch_col_idx);
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
        const maybe_colpair = try this.q.db.queryNextCol(this, idx);
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

    fn implInto(row: *QueryRow, comptime DestType: type, dest: *DestType) !void {
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

    pub fn into(row: *QueryRow, dest: anytype) !void {
        const ti = @typeInfo(@TypeOf(dest));
        switch (ti) {
            .Pointer => {},
            else => {
                @compileError("dest must be pointer to struct, found:" ++ @typeName(@TypeOf(dest)));
            },
        }
        try row.implInto(@typeInfo(@TypeOf(dest)).Pointer.child, dest);
    }

    /// DestType must be initalbe by DestType{}, i.e., provide default value for all fields
    pub fn initInto(row: *QueryRow, comptime DestType: type) !DestType {
        var v: DestType = DestType{};
        try row.implInto(DestType, &v);
        return v;
    }

    /// DestType will be allocated by allocator and owned by user
    pub fn initIntoAlloc(row: *QueryRow, comptime DestType: type, allocator: std.mem.Allocator) !DestType {
        const v = try allocator.create(DestType);
        try row.implInto(DestType, v);
        return v;
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

// query/stmt builder

pub const QualifiedTableName = struct {
    schema_name: ?[]const u8 = null,
    table_or_alias_name: []const u8,
};

pub const QualifiedColName = struct {
    table_name: ?QualifiedTableName = null,
    col_name: []const u8,
};

pub const UnaryStmtExpr = struct {
    op: enum { minus, plus },
    sub_expr: *const StmtExpr,
};

pub const BinaryStmtExpr = struct {
    op: enum { plus, minus, multiply, divide, mod, eq, not_eq, like, not_like, is_distinct_from, is_not_distinct_from },
    sub_expr_left: *const StmtExpr,
    sub_expr_right: *const StmtExpr,
};

pub const SuffixStmtExpr = struct {
    op: enum { isnull, notnull },
    sub_expr: *const StmtExpr,
};

pub const CastStmtExpr = struct {
    sub_expr: *const StmtExpr,
    as_type: ValueType,
};

pub const BetweenStmtExpr = struct {
    op: enum { between, not_between },
    target_expr: *const StmtExpr,
    range_start_expr: *const StmtExpr,
    range_end_expr: *const StmtExpr,
};

pub const InStmtExpr = struct {
    pub const Set = union(enum) {
        sub_select: *const SelectStmt,
        exprs: []const StmtExpr,
        qualified_table: QualifiedTableName,
        function_call: FunctionCallClause,
    };

    op: enum { in, not_in },
    set: Set,
};

pub const ExistStmtExpr = struct {
    op: enum { exist, not_exist },
    select_stmt: *const SelectStmt,
};

pub const StmtExpr = union(enum) {
    literal_value: ValueType,
    qualified_col: QualifiedColName,
    unary_expr: UnaryStmtExpr,
    binary_expr: BinaryStmtExpr,
    suffix_expr: SuffixStmtExpr,
    exist_expr: ExistStmtExpr,
    cast_expr: CastStmtExpr,
    between_expr: BetweenStmtExpr,
    in_expr: InStmtExpr,
};

pub const FunctionCallClause = struct {
    schema_name: ?[]const u8 = null,
    func_name: []const u8,
    exprs: []const StmtExpr,
};

pub const TableOrSubQuery = union(enum) {
    pub const Table = struct {
        schema_name: ?[]const u8 = null,
        name: []const u8,
        as: ?[]const u8 = null,
        index: ?union(enum) {
            indexed_by: []const u8,
            not_indexed: void,
        } = null,
    };
    pub const TableFuncValue = struct {
        func_call: FunctionCallClause,
        as: ?[]const u8 = null,
    };
    pub const SelectAs = struct {
        sub_select: *const SelectStmt,
        as: ?[]const u8 = null,
    };

    table: Table,
    table_func_value: TableFuncValue,
    sub_select: SelectAs,
    join_clause: JoinClause,
    table_or_sub_queries: []const TableOrSubQuery,
};

pub const JoinClause = struct {
    pub const JoinOperator = union(enum) {
        in_or_out: struct {
            natural: bool = false,
            t: union(enum) {
                outer: enum { left, right, full },
                inner: void,
            },
        },
        cross: void,
    };
    pub const JoinConstraint = union(enum) {
        on: *const StmtExpr,
        using: []QualifiedColName,
    };
    pub const NextJoin = struct {
        join_op: JoinOperator,
        table_or_sub_query: *const TableOrSubQuery,
        join_constraint: JoinConstraint,
    };

    table_or_sub_query: *const TableOrSubQuery,
    next_join: []const NextJoin,
};

pub const FromClause = union(enum) {
    table_or_sub_query: TableOrSubQuery,
    join_clause: JoinClause,
};

pub const SelectCoreClause = struct {
    pub const ResultCol = union(enum) {
        pub const Expr = struct {
            expr: *const StmtExpr,
            as: ?[]const u8 = null,
        };

        expr: Expr,
        star: void,
        table_star: []const u8,
    };

    distinct: bool = false,
    result_cols: []const ResultCol,
    from: ?FromClause = null,
    where: ?*const StmtExpr = null,
    group_by: ?[]const StmtExpr = null,
    having: ?*const StmtExpr = null,
};

pub const LimitClause = struct {
    expr: *const StmtExpr,
    offset: ?*const StmtExpr,
};

pub const OrderByClause = struct {
    expr: *const StmtExpr,
    collate: ?[]const u8 = null,
    order: ?Order = null,
    nulls_handling: enum { first, last },
};

pub const SelectStmt = struct {
    pub const CompoundSelect = struct {
        select_core: SelectCoreClause,
        compound: ?enum {
            union_,
            union_all,
            intersect,
            except,
        } = null,
    };
    compound_select: []const CompoundSelect,
    order_by: ?OrderByClause = null,
    limit: ?LimitClause = null,
};

pub const InsertStmt = struct {
    pub const Payload = union(enum) {
        values: []ValueType,
        sub_select: SelectStmt,
        // TODO: need to support this?
        default_values: void,
    };

    verb: enum { replace, insert },
    conflict_handling: Constraint.ConflictHandling,
    schema_name: ?[]const u8 = null,
    table_name: []const u8,
    alias: ?[]const u8 = null,
    col_names: ?[]const []const u8 = null,
    payload: Payload,
    upsert_clause: ?[]const []const u8 = null,
};

pub const UpdateStmt = struct {
    pub const From = union(enum) {
        table: []const u8,
        sub_select: SelectStmt,
    };

    conflict_handling: Constraint.ConflictHandling,
    schema_name: ?[]const u8 = null,
    table_name: []const u8,
    alias: ?[]const u8 = null,
    col_name: []const u8,
    value: ValueType,
    from: ?From = null,
    where: ?[]const []const u8 = null,
};

pub const Stmt = union(enum) {
    select: SelectStmt,
    insert: InsertStmt,
    update: UpdateStmt,
};

pub const QueryBuilder = struct {
    // all implXXX returns string slice not owned by user, i.e., they will expire when next build or deinit
    pub const VTable = struct {
        implReset: *const fn (ctx: *anyopaque) void,
        implGetSql: *const fn (ctx: *anyopaque) []const u8,
        implGetLocalType: *const fn (ctx: *anyopaque, value_type: ValueType) []const u8,
        implBuildStmtExpr: *const fn (ctx: *anyopaque, expr: StmtExpr) Error!void,
        implBuildSelectStmt: *const fn (ctx: *anyopaque, select_stmt: SelectStmt) Error!void,
        implBuildFunctionCall: *const fn (ctx: *anyopaque, func_call: FunctionCallClause) Error!void,
    };

    ctx: *anyopaque,
    vtable: VTable,

    // wrappers
    pub inline fn reset(this: *QueryBuilder) void {
        this.vtable.implReset(this.ctx);
    }

    pub inline fn getSql(this: *QueryBuilder) []const u8 {
        return this.vtable.implGetSql(this.ctx);
    }

    pub inline fn getLocalType(this: *QueryBuilder, value_type: ValueType) []const u8 {
        return this.vtable.implGetLocalType(this.ctx, value_type);
    }

    pub inline fn buildStmtExpr(this: *QueryBuilder, expr: StmtExpr) Error!void {
        try this.vtable.implBuildStmtExpr(this.ctx, expr);
    }

    pub inline fn buildSelectStmt(this: *QueryBuilder, select_stmt: SelectStmt) Error!void {
        try this.vtable.implBuildSelectStmt(this.ctx, select_stmt);
    }

    pub inline fn buildFunctionCall(this: *QueryBuilder, func_call: FunctionCallClause) Error!void {
        try this.vtable.implBuildFunctionCall(this.ctx, func_call);
    }
};

pub const GeneralSQLQueryBuilder = struct {
    allocator: std.mem.Allocator,
    sql_buf: std.ArrayList(u8),
    vtable: QueryBuilder.VTable,

    pub fn init(allocator: std.mem.Allocator) GeneralSQLQueryBuilder {
        return GeneralSQLQueryBuilder{
            .allocator = allocator,
            .sql_buf = std.ArrayList(u8).init(allocator),
            .vtable = QueryBuilder.VTable{
                .implReset = reset,
                .implGetSql = getSql,
                .implGetLocalType = getLocalType,
                .implBuildStmtExpr = buildStmtExpr,
                .implBuildSelectStmt = buildSelectStmt,
                .implBuildFunctionCall = buildFunctionCall,
            },
        };
    }

    pub fn deinit(this: *GeneralSQLQueryBuilder) void {
        this.sql_buf.deinit();
    }

    pub fn queryBuilder(this: *GeneralSQLQueryBuilder) QueryBuilder {
        return .{
            .ctx = this,
            .vtable = this.vtable,
        };
    }

    pub fn buildQualifiedTableName(sql_writer: std.ArrayList(u8).Writer, table_name: QualifiedTableName) !void {
        if (table_name.schema_name) |schema_name| {
            try sql_writer.print("\"{s}\".", .{schema_name});
        }
        try sql_writer.print("\"{s}\"", .{table_name.table_or_alias_name});
    }

    pub fn buildQualifiedColName(sql_writer: std.ArrayList(u8).Writer, col_name: QualifiedColName) !void {
        if (col_name.table_name) |table_name| {
            try buildQualifiedTableName(sql_writer, table_name);
            try sql_writer.print(".", .{});
        }
        try sql_writer.print("\"{s}\"", .{col_name.col_name});
    }

    // vtable fns
    fn reset(ctx: *anyopaque) void {
        const b: *GeneralSQLQueryBuilder = @ptrCast(@alignCast(ctx));
        b.sql_buf.clearRetainingCapacity();
    }

    fn getSql(ctx: *anyopaque) []const u8 {
        const b: *GeneralSQLQueryBuilder = @ptrCast(@alignCast(ctx));
        return b.sql_buf.items;
    }

    fn getLocalType(ctx: *anyopaque, value_type: ValueType) []const u8 {
        _ = ctx;
        std.debug.print("Using the general getLocalType impl, usually is not what you want! This is only for generaldb testing. \n", .{});
        return @tagName(value_type);
    }

    fn buildStmtExpr(ctx: *anyopaque, expr: StmtExpr) Error!void {
        const b: *GeneralSQLQueryBuilder = @ptrCast(@alignCast(ctx));
        const sql_writer = b.sql_buf.writer();
        switch (expr) {
            .literal_value => |lv| try lv.toSql(sql_writer),

            .qualified_col => |qc| try buildQualifiedColName(sql_writer, qc),

            .unary_expr => |ue| {
                switch (ue.op) {
                    .minus => try sql_writer.print("- ", .{}),
                    .plus => try sql_writer.print("+ ", .{}),
                }
                try buildStmtExpr(ctx, ue.sub_expr.*);
            },

            .binary_expr => |be| {
                try buildStmtExpr(ctx, be.sub_expr_left.*);
                switch (be.op) {
                    .plus => try sql_writer.print(" + ", .{}),
                    .minus => try sql_writer.print(" - ", .{}),
                    .multiply => try sql_writer.print(" * ", .{}),
                    .divide => try sql_writer.print(" / ", .{}),
                    .mod => try sql_writer.print(" % ", .{}),
                    .eq => try sql_writer.print(" = ", .{}),
                    .not_eq => try sql_writer.print(" != ", .{}),
                    .like => try sql_writer.print(" LIKE ", .{}),
                    .not_like => try sql_writer.print(" NOT LIKE ", .{}),
                    .is_distinct_from => try sql_writer.print(" IS DISTINCT FROM ", .{}),
                    .is_not_distinct_from => try sql_writer.print(" IS NOT DISTINCT FROM ", .{}),
                }
                try buildStmtExpr(ctx, be.sub_expr_right.*);
            },

            .suffix_expr => |se| {
                try buildStmtExpr(ctx, se.sub_expr.*);
                switch (se.op) {
                    .isnull => try sql_writer.print(" IS NULL", .{}),
                    .notnull => try sql_writer.print(" IS NOT NULL", .{}),
                }
            },

            .exist_expr => |ee| {
                switch (ee.op) {
                    .exist => try sql_writer.print("EXIST (", .{}),
                    .not_exist => try sql_writer.print("NOT EXIST (", .{}),
                }
                try buildSelectStmt(ctx, ee.select_stmt.*);
                try sql_writer.print(")", .{});
            },

            .cast_expr => |ce| {
                try buildStmtExpr(ctx, ce.sub_expr.*);
                try sql_writer.print(" AS {s}", .{getLocalType(ctx, ce.as_type)});
            },

            .between_expr => |be| {
                try buildStmtExpr(ctx, be.target_expr.*);
                switch (be.op) {
                    .between => try sql_writer.print(" BETWEEN ", .{}),
                    .not_between => try sql_writer.print(" NOT BETWEEN ", .{}),
                }
                try buildStmtExpr(ctx, be.range_start_expr.*);
                try sql_writer.print(" AND ", .{});
                try buildStmtExpr(ctx, be.range_end_expr.*);
            },

            .in_expr => |ie| {
                switch (ie.op) {
                    .in => try sql_writer.print("IN ", .{}),
                    .not_in => try sql_writer.print("NOT IN ", .{}),
                }
                switch (ie.set) {
                    .sub_select => |s| {
                        try sql_writer.print("( ", .{});
                        try buildSelectStmt(ctx, s.*);
                        try sql_writer.print(" )", .{});
                    },
                    .qualified_table => |qt| try buildQualifiedTableName(sql_writer, qt),
                    .exprs => |e_arr| {
                        try sql_writer.print("( ", .{});
                        for (e_arr, 0..) |e, i| {
                            if (i != 0) {
                                try sql_writer.print(", ", .{});
                            }
                            try buildStmtExpr(ctx, e);
                        }
                        try sql_writer.print(" )", .{});
                    },
                    .function_call => |fc| {
                        try buildFunctionCall(ctx, fc);
                    },
                }
            },
        }
    }

    fn buildSelectStmt(ctx: *anyopaque, select_stmt: SelectStmt) Error!void {
        const b: *GeneralSQLQueryBuilder = @ptrCast(@alignCast(ctx));
        const sql_writer = b.sql_buf.writer();
        for (select_stmt.compound_select, 0..) |cs, i| {
            if (i != 0) {
                try sql_writer.print(" ", .{});
            }
            try buildCompoundSelect(ctx, cs);
        }
        if (select_stmt.order_by) |order_by| {
            _ = order_by;
        }
        if (select_stmt.limit) |limit| {
            _ = limit;
        }
    }

    fn buildCompoundSelect(ctx: *anyopaque, compound_select: SelectStmt.CompoundSelect) Error!void {
        const b: *GeneralSQLQueryBuilder = @ptrCast(@alignCast(ctx));
        const sql_writer = b.sql_buf.writer();
        try buildSelectCore(ctx, compound_select.select_core);
        if (compound_select.compound) |compound| {
            switch (compound) {
                .union_ => try sql_writer.print(" UNION", .{}),
                .union_all => try sql_writer.print(" UNION ALL", .{}),
                .intersect => try sql_writer.print(" INTERSECT", .{}),
                .except => try sql_writer.print(" EXCEPT", .{}),
            }
        }
    }

    fn buildSelectCore(ctx: *anyopaque, select_core: SelectCoreClause) Error!void {
        const b: *GeneralSQLQueryBuilder = @ptrCast(@alignCast(ctx));
        const sql_writer = b.sql_buf.writer();
        try sql_writer.print("SELECT ", .{});
        if (select_core.distinct) {
            try sql_writer.print("DISTINCT ", .{});
        }
        for (select_core.result_cols) |result_col| {
            switch (result_col) {
                .expr => |e| {
                    try buildStmtExpr(ctx, e.expr.*);
                    if (e.as) |a| {
                        try sql_writer.print(" AS '{s}'", .{a});
                    }
                },
                .star => try sql_writer.print("*", .{}),
                .table_star => |tsv| try sql_writer.print("\"{s}\".*", .{tsv}),
            }
        }
        if (select_core.from) |from| {
            _ = from;
        }
        if (select_core.where) |where| {
            _ = where;
        }
        if (select_core.group_by) |group_by| {
            _ = group_by;
        }
        if (select_core.having) |having| {
            _ = having;
        }
    }

    fn buildFunctionCall(ctx: *anyopaque, func_call: FunctionCallClause) Error!void {
        const b: *GeneralSQLQueryBuilder = @ptrCast(@alignCast(ctx));
        const sql_writer = b.sql_buf.writer();
        if (func_call.schema_name) |schema_name| {
            try sql_writer.print("{s}.", .{schema_name});
        }
        try sql_writer.print("{s}(", .{func_call.func_name});
        for (func_call.exprs, 0..) |expr, i| {
            if (i != 0) {
                try sql_writer.print(", ", .{});
            }
            try buildStmtExpr(ctx, expr);
        }
        try sql_writer.print(")", .{});
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
    pub const CreateTableOpts = struct {
        if_not_exists: bool = false,
    };
    pub const DropTableOpts = struct {
        if_exists: bool = false,
    };
    pub const RenameTableOpts = struct {};

    pub const AddColumnOpts = struct {};
    pub const DropColumnOpts = struct {};
    pub const RenameColumnOpts = struct {};
    pub const AlterColumnOpts = struct {};

    pub const CreateViewOpts = struct {
        if_not_exists: bool = false,
    };
    pub const DropViewOpts = struct {
        if_exists: bool = false,
    };

    pub const CreateConstraintOpts = struct {};
    pub const DropConstraintOpts = struct {};

    pub const CreateIndexOpts = struct {
        if_not_exists: bool = false,
    };
    pub const DropIndexOpts = struct {
        if_exists: bool = false,
    };
    pub const RenameIndexOpts = struct {};

    implOpen: *const fn (ctx: *anyopaque) Error!void,
    implClose: *const fn (ctx: *anyopaque) void,

    implCreateDatabase: *const fn (ctx: *anyopaque, name: []const u8, opts: CreateDatabaseOpts) Error!void,
    implUseDatabase: *const fn (ctx: *anyopaque, name: []const u8) Error!void,

    implQueryEvaluate: *const fn (ctx: *anyopaque, query: *Query) Error!void,
    implQueryNextRow: *const fn (ctx: *anyopaque) Error!?usize,
    implQueryNextCol: *const fn (ctx: *anyopaque, row: *QueryRow, col_idx: usize) Error!?QueryCol,
    implQueryFinalize: *const fn (ctx: *anyopaque, query: *Query) usize,

    implMigrate: *const fn (ctx: *anyopaque, db: *Db, migration: *Migration, direction: MigrateDirection) Error!void,
    implCreateTable: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_schema: TableSchema, opts: CreateTableOpts) Error!void,
    implDropTable: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, opts: DropTableOpts) Error!void,
    implRenameTable: *const fn (ctx: *anyopaque, db: *Db, chagne_set: *ChangeSet, from_table_name: []const u8, to_table_name: []const u8, opts: RenameTableOpts) Error!void,
    implHasTable: *const fn (ctx: *anyopaque, db: *Db, table_name: []const u8) Error!bool,
    implAddColumn: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, column: TableSchema.Column, opts: AddColumnOpts) Error!void,
    implDropColumn: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, opts: DropColumnOpts) Error!void,
    implRenameColumn: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, from_col_name: []const u8, to_col_name: []const u8, opts: RenameColumnOpts) Error!void,
    implAlterColumn: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, new_type: ValueType, new_col_opts: ColOpts, opts: DbVTable.AlterColumnOpts) Error!void,
    implCreateView: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, view_name: []const u8, view_opts: ViewOpts, select_sql: []const u8, opts: CreateViewOpts) Error!void,
    implDropView: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, view_name: []const u8, opts: DropViewOpts) Error!void,
    implCreateConstraint: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, constraint: Constraint, opts: CreateConstraintOpts) Error!void,
    implDropConstraint: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, constraint_name: []const u8, opts: DropConstraintOpts) Error!void,
    implCreateIndex: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, table_name: []const u8, index: TableSchema.Index, opts: DbVTable.CreateIndexOpts) Error!void,
    implDropIndex: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, index_name: []const u8, opts: DbVTable.DropIndexOpts) Error!void,
    implRenameIndex: *const fn (ctx: *anyopaque, db: *Db, change_set: *ChangeSet, index_name: []const u8, new_index_name: []const u8, opts: DbVTable.RenameIndexOpts) Error!void,
    implHasIndex: *const fn (ctx: *anyopaque, db: *Db, index_name: []const u8) Error!bool,
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

pub fn queryExecute(this: *Db, query: *Query) Error!usize {
    try this.queryEvaluate(query);
    var rit = try query.iterator();
    while (true) {
        if ((try rit.next()) != null) continue else break;
    }
    query.finalize();
    return query.row_affected;
}

pub fn queryExecuteSlice(this: *Db, allocator: std.mem.Allocator, query: []const u8) Error!usize {
    var q = try Query.init(allocator, this, query, null);
    try this.queryEvaluate(&q);
    var rit = try q.iterator();
    while (true) {
        if ((try rit.next()) != null) continue else break;
    }
    q.finalize();
    const row_affected = q.row_affected;
    q.deinit();
    return row_affected;
}

// migration fns

pub const TableAvailablity = union(enum) {
    to_create: struct {
        dependency_map: std.StringHashMap(void),
    },
    existed: void,
};

pub const TableAvailablityMap = struct {
    arena: std.heap.ArenaAllocator,
    map: std.StringArrayHashMap(TableAvailablity),
    to_create_count: usize,

    pub fn deinit(this: *TableAvailablityMap) void {
        this.arena.deinit();
    }
};

/// returned string hashmap of TableAvailablity must be freed by user
pub fn validateTableSchemas(this: *Db, allocator: std.mem.Allocator, table_schemas: []TableSchema) !TableAvailablityMap {
    var arena = std.heap.ArenaAllocator.init(allocator);
    var known_tables = std.StringArrayHashMap(TableAvailablity).init(arena.allocator());
    var buf: [8192]u8 = undefined;

    for (table_schemas) |table_schema| {
        if (known_tables.contains(table_schema.table_name)) {
            const msg = try std.fmt.bufPrint(&buf, "repeated table schema '{s}' found!", .{table_schema.table_name});
            @panic(msg);
        }
        try known_tables.put(
            table_schema.table_name,
            TableAvailablity{
                .to_create = .{
                    .dependency_map = std.StringHashMap(void).init(arena.allocator()),
                },
            },
        );
    }

    for (table_schemas) |table_schema| {
        if (try this.hasTable(table_schema.table_name)) {
            const msg = try std.fmt.bufPrint(&buf, "table '{s}' alreay exists!", .{table_schema.table_name});
            @panic(msg);
        }

        if (table_schema.foreign_keys.len == 0) {
            continue;
        }

        var known_entry = if (known_tables.getPtr(table_schema.table_name)) |e| e else {
            @panic("impossilbe this table disappeared!");
        };

        for (table_schema.foreign_keys) |fk| {
            if (!known_tables.contains(fk.ref_table_name)) {
                if (try this.hasTable(fk.ref_table_name)) {
                    try known_tables.put(fk.ref_table_name, TableAvailablity{ .existed = {} });
                } else {
                    const msg = try std.fmt.bufPrint(&buf, "table '{s}' referenced table '{s}' in foreign key '{?s}', which is neither existing in current db, nor will be created.", .{ table_schema.table_name, fk.ref_table_name, fk.name });
                    @panic(msg);
                }
            }
            switch (known_entry.*) {
                .to_create => {
                    try known_entry.to_create.dependency_map.put(fk.ref_table_name, {});
                },
                .existed => unreachable,
            }
        }
    }

    return .{
        .arena = arena,
        .map = known_tables,
        .to_create_count = table_schemas.len,
    };
}

pub fn freeTableAvailability(table_availability: *TableAvailablityMap) void {
    var it = table_availability.iterator();
    while (it.next()) |e| {
        switch (e.value_ptr.*) {
            .to_create => |*e2| {
                e2.dependency_map.deinit();
            },
            .existed => {},
        }
    }
    table_availability.deinit();
}

fn createTableToChangeSet(this: *Db, change_set: *ChangeSet, table_schema: TableSchema, table_avalibility: *TableAvailablityMap, table_created: *std.StringHashMap(void), opts: DbVTable.CreateTableOpts) Error!void {
    try this.vtable.implCreateTable(this.ctx, this, change_set, table_schema, opts);

    var it = table_avalibility.map.iterator();
    while (it.next()) |e| {
        var ta = if (table_avalibility.map.getPtr(e.key_ptr.*)) |v| v else unreachable;
        _ = ta.to_create.dependency_map.remove(table_schema.table_name);
    }

    table_avalibility.to_create_count -= 1;
    try table_created.put(table_schema.table_name, {});
}

pub fn createTables(this: *Db, change_set: *ChangeSet, table_schemas: []TableSchema, opts: DbVTable.CreateTableOpts) Error!void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var table_created = std.StringHashMap(void).init(arena.allocator());
    defer table_created.deinit();

    var table_avalibility = try this.validateTableSchemas(arena.allocator(), table_schemas);

    while (table_avalibility.to_create_count > 0) {
        const to_create_table_schema = brk: {
            var it = table_avalibility.map.iterator();
            while (it.next()) |e| {
                if (table_created.contains(e.key_ptr.*)) {
                    continue;
                }
                switch (e.value_ptr.*) {
                    .existed => continue,
                    .to_create => {
                        if (e.value_ptr.to_create.dependency_map.count() == 0) {
                            for (0..table_schemas.len) |j| {
                                if (std.mem.eql(u8, table_schemas[j].table_name, e.key_ptr.*)) {
                                    break :brk table_schemas[j];
                                }
                            }
                            std.debug.print("impossible, can not find table {s} in table_schemas", .{e.key_ptr.*});
                            unreachable;
                        }
                    },
                }
            }
            SchemaUtil.dumpTableDependencies(table_schemas);
            @panic("Can not find any table to start from table_schemas provided, there is circular dependencies, please check above dump list!");
        };
        try this.createTableToChangeSet(change_set, to_create_table_schema, &table_avalibility, &table_created, opts);
    }
}

pub inline fn createTable(this: *Db, change_set: *ChangeSet, ent: TableSchema, opts: DbVTable.CreateTableOpts) Error!void {
    var table_schemas: [1]TableSchema = undefined;
    table_schemas[0] = ent;
    try this.createTables(change_set, &table_schemas, opts);
}

pub inline fn dropTable(this: *Db, change_set: *ChangeSet, table_name: []const u8, opts: DbVTable.DropTableOpts) Error!void {
    try this.vtable.implDropTable(this.ctx, this, change_set, table_name, opts);
}

pub inline fn renameTable(this: *Db, change_set: *ChangeSet, from_table_name: []const u8, to_table_name: []const u8, opts: DbVTable.RenameTableOpts) Error!void {
    try this.vtable.implRenameTable(this.ctx, this, change_set, from_table_name, to_table_name, opts);
}

pub inline fn hasTable(this: *Db, table_name: []const u8) Error!bool {
    return try this.vtable.implHasTable(this.ctx, this, table_name);
}

pub inline fn addColumn(this: *Db, change_set: *ChangeSet, table_name: []const u8, column: TableSchema.Column, opts: DbVTable.AddColumnOpts) Error!void {
    try this.vtable.implAddColumn(this.ctx, this, change_set, table_name, column, opts);
}

pub inline fn dropColumn(this: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, opts: DbVTable.DropColumnOpts) Error!void {
    try this.vtable.implDropColumn(this.ctx, this, change_set, table_name, col_name, opts);
}

pub inline fn renameColumn(this: *Db, change_set: *ChangeSet, table_name: []const u8, from_col_name: []const u8, to_col_name: []const u8, opts: DbVTable.RenameColumnOpts) Error!void {
    try this.vtable.implRenameColumn(this.ctx, this, change_set, table_name, from_col_name, to_col_name, opts);
}

pub inline fn alterColumn(this: *Db, change_set: *ChangeSet, table_name: []const u8, col_name: []const u8, new_type: ValueType, new_col_opts: ColOpts, opts: DbVTable.AlterColumnOpts) Error!void {
    try this.vtable.implAlterColumn(this.ctx, this, change_set, table_name, col_name, new_type, new_col_opts, opts);
}

pub inline fn createView(this: *Db, change_set: *ChangeSet, view_name: []const u8, view_opts: ViewOpts, select_sql: []const u8, opts: DbVTable.CreateViewOpts) Error!void {
    try this.vtable.implCreateView(this.ctx, this, change_set, view_name, view_opts, select_sql, opts);
}

pub inline fn dropView(this: *Db, change_set: *ChangeSet, view_name: []const u8, opts: DbVTable.DropViewOpts) Error!void {
    try this.vtable.implDropView(this.ctx, this, change_set, view_name, opts);
}

pub inline fn createConstraint(this: *Db, change_set: *ChangeSet, table_name: []const u8, constraint: Constraint, opts: DbVTable.CreateConstraintOpts) Error!void {
    try this.vtable.implCreateConstraint(this.ctx, this, change_set, table_name, constraint, opts);
}

pub inline fn dropConstraint(this: *Db, change_set: *ChangeSet, table_name: []const u8, constraint_name: []const u8, opts: DbVTable.DropConstraintOpts) Error!void {
    try this.vtable.implDropConstraint(this.ctx, this, change_set, table_name, constraint_name, opts);
}

pub inline fn createIndex(this: *Db, change_set: *ChangeSet, table_name: []const u8, index: TableSchema.Index, opts: DbVTable.CreateIndexOpts) Error!void {
    try this.vtable.implCreateIndex(this.ctx, this, change_set, table_name, index, opts);
}

pub inline fn dropIndex(this: *Db, change_set: *ChangeSet, index_name: []const u8, opts: DbVTable.DropIndexOpts) Error!void {
    try this.vtable.implDropIndex(this.ctx, this, change_set, index_name, opts);
}

pub inline fn renameIndex(this: *Db, change_set: *ChangeSet, index_name: []const u8, new_index_name: []const u8, opts: DbVTable.RenameIndexOpts) Error!void {
    try this.vtable.implRenameIndex(this.ctx, this, change_set, index_name, new_index_name, opts);
}

pub inline fn hasIndex(this: *Db, index_name: []const u8) Error!bool {
    return this.vtable.implHasIndex(this.ctx, this, index_name);
}

// all tests

test "genSchema" {
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
            .{ "id", ColOpts{ .auto_increment = true } },
            .{ "company_id", ColOpts{ .unique = true } },
        };
        pub const relations = .{
            .{ "company_id", Company, "id", RelationOpts{ .association = .one_to_one } },
            .{ "skill_id", Skill, "id", RelationOpts{ .association = .many_to_many } },
        };
        pub const checks = .{.{"name NOT IN ('User', 'Company')"}};
    };
    {
        const user_table = SchemaUtil.genSchemaOne(User);

        try testing.expectEqualSlices(u8, "User", user_table.table_name);
        try testing.expectEqualDeep(&[_]TableSchema.Column{
            TableSchema.Column{
                .name = "id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{ .unique = true, .nullable = false, .auto_increment = true },
            },
            TableSchema.Column{
                .name = "name",
                .type = ValueType{ .TEXT = undefined },
                .opts = ColOpts{},
            },
            TableSchema.Column{
                .name = "sex",
                .type = ValueType{ .BOOL = undefined },
                .opts = ColOpts{},
            },
            TableSchema.Column{
                .name = "hobby",
                .type = ValueType{ .TEXT = undefined },
                .opts = ColOpts{ .nullable = true },
            },
            TableSchema.Column{
                .name = "company_id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{ .unique = true },
            },
            TableSchema.Column{
                .name = "skill_id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{},
            },
        }, user_table.columns);

        try testing.expectEqualDeep(&[_]Constraint.ForeignKey{
            Constraint.ForeignKey{
                .name = "fk_User_Company_id_o2o",
                .col_names = &[_][]const u8{"company_id"},
                .ref_table_name = "Company",
                .ref_table_col_names = &[_][]const u8{"id"},
                .opts = RelationOpts{
                    .association = .one_to_one,
                },
            },
        }, user_table.foreign_keys);

        try testing.expectEqualDeep(
            Constraint.PrimaryKey{
                .col_names = &[_][]const u8{"id"},
            },
            user_table.primary_key,
        );
    }
    {
        const tables = SchemaUtil.genSchema(.{ User, Company, Skill });
        try testing.expectEqual(4, tables.len);

        const user_table = tables[0];
        const company_table = tables[1];
        const skill_table = tables[2];
        const m2m_table = tables[3];

        // user_table

        try testing.expectEqualSlices(u8, "User", user_table.table_name);
        try testing.expectEqualDeep(&[_]TableSchema.Column{
            TableSchema.Column{
                .name = "id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{ .unique = true, .nullable = false, .auto_increment = true },
            },
            TableSchema.Column{
                .name = "name",
                .type = ValueType{ .TEXT = undefined },
                .opts = ColOpts{},
            },
            TableSchema.Column{
                .name = "sex",
                .type = ValueType{ .BOOL = undefined },
                .opts = ColOpts{},
            },
            TableSchema.Column{
                .name = "hobby",
                .type = ValueType{ .TEXT = undefined },
                .opts = ColOpts{ .nullable = true },
            },
            TableSchema.Column{
                .name = "company_id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{ .unique = true },
            },
            TableSchema.Column{
                .name = "skill_id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{},
            },
        }, user_table.columns);

        try testing.expectEqualDeep(&[_]Constraint.ForeignKey{
            Constraint.ForeignKey{
                .name = "fk_User_Company_id_o2o",
                .col_names = &[_][]const u8{"company_id"},
                .ref_table_name = "Company",
                .ref_table_col_names = &[_][]const u8{"id"},
                .opts = RelationOpts{
                    .association = .one_to_one,
                },
            },
        }, user_table.foreign_keys);

        try testing.expectEqualDeep(
            Constraint.PrimaryKey{
                .col_names = &[_][]const u8{"id"},
            },
            user_table.primary_key,
        );

        try testing.expectEqualDeep(
            &[_]Constraint.Check{
                Constraint.Check{
                    .expr = "name NOT IN ('User', 'Company')",
                },
            },
            user_table.checks,
        );

        // company_table

        try testing.expectEqualSlices(u8, "Company", company_table.table_name);
        try testing.expectEqualDeep(&[_]TableSchema.Column{
            TableSchema.Column{
                .name = "id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{ .unique = true, .nullable = false },
            },
            TableSchema.Column{
                .name = "name",
                .type = ValueType{ .TEXT = undefined },
                .opts = ColOpts{},
            },
        }, company_table.columns);

        try testing.expectEqualDeep(
            Constraint.PrimaryKey{
                .col_names = &[_][]const u8{"id"},
            },
            company_table.primary_key,
        );

        try testing.expectEqual(0, company_table.foreign_keys.len);

        try testing.expectEqualDeep(&[_]TableSchema.Index{
            TableSchema.Index{
                .name = "idx_Company_name",
                .keys = &[_][]const u8{"name"},
                .opts = IndexOpts{ .unique = true, .sortings = &[1]IndexOpts.Sorting{.desc} },
            },
        }, company_table.indexes);

        // skill_table

        try testing.expectEqualSlices(u8, "Skill", skill_table.table_name);
        try testing.expectEqualDeep(&[_]TableSchema.Column{
            TableSchema.Column{
                .name = "id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{ .unique = true, .nullable = false },
            },
            TableSchema.Column{
                .name = "name",
                .type = ValueType{ .TEXT = undefined },
                .opts = ColOpts{},
            },
        }, skill_table.columns);

        try testing.expectEqualDeep(
            Constraint.PrimaryKey{
                .col_names = &[_][]const u8{"id"},
            },
            skill_table.primary_key,
        );
        try testing.expectEqual(0, skill_table.foreign_keys.len);
        try testing.expectEqual(0, skill_table.indexes.len);

        // m2m_table

        try testing.expectEqualSlices(u8, "m2m_Skill_id_User_skill_id", m2m_table.table_name);
        try testing.expectEqualDeep(&[_]TableSchema.Column{
            TableSchema.Column{
                .name = "id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{ .unique = true, .nullable = false },
            },
            TableSchema.Column{
                .name = "User_skill_id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{},
            },
            TableSchema.Column{
                .name = "Skill_id",
                .type = ValueType{ .INT64 = undefined },
                .opts = ColOpts{},
            },
        }, m2m_table.columns);

        try testing.expectEqualDeep(
            Constraint.PrimaryKey{
                .col_names = &[_][]const u8{"id"},
            },
            m2m_table.primary_key,
        );

        try testing.expectEqualDeep(&[_]Constraint.ForeignKey{
            Constraint.ForeignKey{
                .name = "fk_m2m_Skill_id_User_skill_id_0",
                .col_names = &[_][]const u8{"User_skill_id"},
                .ref_table_name = "User",
                .ref_table_col_names = &[_][]const u8{"skill_id"},
                .opts = RelationOpts{ .association = .many_to_one },
            },
            Constraint.ForeignKey{
                .name = "fk_m2m_Skill_id_User_skill_id_1",
                .col_names = &[_][]const u8{"Skill_id"},
                .ref_table_name = "Skill",
                .ref_table_col_names = &[_][]const u8{"id"},
                .opts = RelationOpts{ .association = .many_to_one },
            },
        }, m2m_table.foreign_keys);

        try testing.expectEqual(0, m2m_table.indexes.len);
    }
}

test "query_builder_expr" {
    var gsqb = GeneralSQLQueryBuilder.init(testing.allocator);
    defer gsqb.deinit();
    var qb = gsqb.queryBuilder();

    const expected_expr_list = .{
        // literal_value
        .{ "5", StmtExpr{ .literal_value = ValueType{ .INT8 = 5 } } },
        .{ "5", StmtExpr{ .literal_value = ValueType{ .INT16 = 5 } } },
        .{ "5", StmtExpr{ .literal_value = ValueType{ .INT32 = 5 } } },
        .{ "5", StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } } },
        .{ "5", StmtExpr{ .literal_value = ValueType{ .UINT8 = 5 } } },
        .{ "5", StmtExpr{ .literal_value = ValueType{ .UINT16 = 5 } } },
        .{ "5", StmtExpr{ .literal_value = ValueType{ .UINT32 = 5 } } },
        .{ "5", StmtExpr{ .literal_value = ValueType{ .UINT64 = 5 } } },
        .{ "5.1", StmtExpr{ .literal_value = ValueType{ .FLOAT32 = .{ .v = 5.1, .precision = 1 } } } },
        .{ "5.10", StmtExpr{ .literal_value = ValueType{ .FLOAT32 = .{ .v = 5.1, .precision = 2 } } } },
        .{ "5", StmtExpr{ .literal_value = ValueType{ .FLOAT32 = .{ .v = 5.0, .precision = 0 } } } },
        .{ "5.1", StmtExpr{ .literal_value = ValueType{ .FLOAT64 = .{ .v = 5.1, .precision = 1 } } } },
        .{ "'hello,5'", StmtExpr{ .literal_value = ValueType{ .TEXT = "hello,5" } } },
        // qualified_col
        .{ "\"name\"", StmtExpr{ .qualified_col = .{ .col_name = "name" } } },
        .{ "\"User\".\"name\"", StmtExpr{
            .qualified_col = .{
                .table_name = .{ .table_or_alias_name = "User" },
                .col_name = "name",
            },
        } },
        .{ "\"MyDb\".\"User\".\"name\"", StmtExpr{
            .qualified_col = .{
                .table_name = .{ .schema_name = "MyDb", .table_or_alias_name = "User" },
                .col_name = "name",
            },
        } },
        // unary_expr
        .{ "- 5", StmtExpr{
            .unary_expr = .{
                .op = .minus,
                .sub_expr = &StmtExpr{
                    .literal_value = ValueType{ .INT64 = 5 },
                },
            },
        } },
        .{ "+ 5", StmtExpr{
            .unary_expr = .{
                .op = .plus,
                .sub_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        // binary_expr
        .{ "1 + 5", StmtExpr{
            .binary_expr = .{
                .op = .plus,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 - 5", StmtExpr{
            .binary_expr = .{
                .op = .minus,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 * 5", StmtExpr{
            .binary_expr = .{
                .op = .multiply,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 / 5", StmtExpr{
            .binary_expr = .{
                .op = .divide,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 % 5", StmtExpr{
            .binary_expr = .{
                .op = .mod,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 = 5", StmtExpr{
            .binary_expr = .{
                .op = .eq,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 != 5", StmtExpr{
            .binary_expr = .{
                .op = .not_eq,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 LIKE 5", StmtExpr{
            .binary_expr = .{
                .op = .like,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 NOT LIKE 5", StmtExpr{
            .binary_expr = .{
                .op = .not_like,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 IS DISTINCT FROM 5", StmtExpr{
            .binary_expr = .{
                .op = .is_distinct_from,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        .{ "1 IS NOT DISTINCT FROM 5", StmtExpr{
            .binary_expr = .{
                .op = .is_not_distinct_from,
                .sub_expr_left = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .sub_expr_right = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
            },
        } },
        // suffic_expr
        .{ "1 IS NULL", StmtExpr{
            .suffix_expr = .{
                .op = .isnull,
                .sub_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
            },
        } },
        .{ "1 IS NOT NULL", StmtExpr{
            .suffix_expr = .{
                .op = .notnull,
                .sub_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
            },
        } },
        // exist_expr
        .{ "EXIST (SELECT 1)", StmtExpr{
            .exist_expr = .{
                .op = .exist,
                .select_stmt = &SelectStmt{
                    .compound_select = &[_]SelectStmt.CompoundSelect{
                        SelectStmt.CompoundSelect{
                            .select_core = .{
                                .result_cols = &[_]SelectCoreClause.ResultCol{
                                    SelectCoreClause.ResultCol{
                                        .expr = SelectCoreClause.ResultCol.Expr{
                                            .expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        } },
        .{ "NOT EXIST (SELECT 1)", StmtExpr{
            .exist_expr = .{
                .op = .not_exist,
                .select_stmt = &SelectStmt{
                    .compound_select = &[_]SelectStmt.CompoundSelect{
                        SelectStmt.CompoundSelect{
                            .select_core = .{
                                .result_cols = &[_]SelectCoreClause.ResultCol{
                                    SelectCoreClause.ResultCol{
                                        .expr = SelectCoreClause.ResultCol.Expr{
                                            .expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        } },
        // cast_expr
        .{ "5 AS TEXT", StmtExpr{
            .cast_expr = .{
                .sub_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
                .as_type = ValueType.getUndefined([]const u8),
            },
        } },
        // between_expr
        .{ "5 BETWEEN 1 AND 10", StmtExpr{
            .between_expr = .{
                .op = .between,
                .target_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
                .range_start_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .range_end_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 10 } },
            },
        } },
        .{ "5 NOT BETWEEN 1 AND 10", StmtExpr{
            .between_expr = .{
                .op = .not_between,
                .target_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 5 } },
                .range_start_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                .range_end_expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 10 } },
            },
        } },
        // in_expr
        .{ "IN ( SELECT 1 )", StmtExpr{
            .in_expr = .{
                .op = .in,
                .set = .{
                    .sub_select = &SelectStmt{
                        .compound_select = &[_]SelectStmt.CompoundSelect{
                            SelectStmt.CompoundSelect{
                                .select_core = .{
                                    .result_cols = &[_]SelectCoreClause.ResultCol{
                                        SelectCoreClause.ResultCol{
                                            .expr = SelectCoreClause.ResultCol.Expr{
                                                .expr = &StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        } },
        .{ "NOT IN \"User\"", StmtExpr{
            .in_expr = .{
                .op = .not_in,
                .set = .{
                    .qualified_table = .{ .table_or_alias_name = "User" },
                },
            },
        } },
        .{ "NOT IN ( 1, 2, 3 )", StmtExpr{
            .in_expr = .{
                .op = .not_in,
                .set = .{
                    .exprs = &[_]StmtExpr{
                        StmtExpr{ .literal_value = ValueType{ .INT64 = 1 } },
                        StmtExpr{ .literal_value = ValueType{ .INT64 = 2 } },
                        StmtExpr{ .literal_value = ValueType{ .INT64 = 3 } },
                    },
                },
            },
        } },
        .{ "NOT IN sqrt(4)", StmtExpr{
            .in_expr = .{
                .op = .not_in,
                .set = .{
                    .function_call = .{
                        .func_name = "sqrt",
                        .exprs = &[_]StmtExpr{
                            StmtExpr{ .literal_value = ValueType{ .INT64 = 4 } },
                        },
                    },
                },
            },
        } },
    };

    inline for (0..expected_expr_list.len) |i| {
        const expected_str = expected_expr_list[i][0];
        const expr = expected_expr_list[i][1];
        qb.reset();
        try qb.buildStmtExpr(expr);
        try testing.expectEqualSlices(u8, expected_str, qb.getSql());
    }
}
