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
    col_opts: []ColOpts = undefined,
    primary_key_names: [][]const u8 = undefined,
    has_foreign_key: bool = false,
    foreign_key_names: [][]const u8 = undefined,
    foreign_key_col_names: [][]const u8 = undefined,
    foreign_key_target_table_names: [][]const u8 = undefined,
    foreign_key_target_col_names: [][]const u8 = undefined,
    foreign_key_relation_optss: []RelationOpts = undefined,
    has_indexes: bool = false,
    index_names: [][]const u8 = undefined,
    index_keys: [][]const u8 = undefined,
    index_optss: []IndexOpts = undefined,
    relation_checked: bool = false,
    denpendency_list: [][]const u8 = undefined,
};

pub const ColOpts = struct {
    nullable: bool = true,
    unique: bool = false,
};

pub const RelationOpts = struct {
    cardinality: enum { one_to_one, many_to_one, many_to_many } = .one_to_one,
    delete_action: enum { cascade, restrict, no_action, set_null, set_default } = .cascade,
    update_action: enum { cascade, restrict, no_action, set_null, set_default } = .cascade,
};

pub const IndexOpts = struct {};

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
                        if (decl_relation[3].cardinality == .many_to_many) {
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

    fn calcForeignKeyName(comptime table_name: []const u8, comptime i: usize, comptime decl_relation: anytype) []const u8 {
        comptime {
            if (!isTuple(@TypeOf(decl_relation))) {
                @compileError("each entry in relations must be tuple, fund:" ++ @typeName(@TypeOf(decl_relation)));
            }
            assertValidRelationTuple(table_name, i, decl_relation);
            const target_table_name = getLastTypeName(decl_relation[1]);
            return "fk_" ++ table_name ++ "_" ++ target_table_name ++ "_" ++ decl_relation[2] ++ "_" ++
                switch (decl_relation[3].cardinality) {
                .one_to_one => "o2o",
                .many_to_one => "m2o",
                .many_to_many => "m2m",
            };
        }
    }

    fn calcForeignKeyArrays(
        comptime table_name: []const u8,
        comptime decl_relations: anytype,
        comptime fk_names: [][]const u8,
        comptime fk_col_names: [][]const u8,
        comptime fk_target_table_names: [][]const u8,
        comptime fk_target_col_names: [][]const u8,
        comptime fk_relation_optss: []RelationOpts,
    ) usize {
        comptime {
            if (!isTuple(@TypeOf(decl_relations))) {
                @compileError("relations must be tuple, fund:" ++ @typeName(@TypeOf(decl_relations)));
            }
            var fk_count: usize = 0;
            for (0..decl_relations.len) |i| {
                const decl_relation = decl_relations[i];
                switch (decl_relation[3].cardinality) {
                    .one_to_one, .many_to_one => {
                        fk_names[i] = calcForeignKeyName(table_name, i, decl_relations[i]);
                        fk_col_names[i] = decl_relations[i][0];
                        fk_target_table_names[i] = getLastTypeName(decl_relations[i][1]);
                        fk_target_col_names[i] = decl_relations[i][2];
                        fk_relation_optss[i] = decl_relations[i][3];
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

    fn calcIndexKey(comptime decl_index: anytype) []const u8 {
        comptime {
            if (!isTuple(@TypeOf(decl_index))) {
                @compileError("each entry in indexes must be tuple, fund:" ++ @typeName(@TypeOf(decl_index)));
            }
            var key: []const u8 = "";
            for (0..decl_index[0].len) |j| {
                if (j == 0) {
                    key = key ++ decl_index[0][j];
                } else {
                    key = key ++ "," ++ decl_index[0][j];
                }
            }
            return key;
        }
    }

    fn calcIndexArrays(
        comptime table_name: []const u8,
        comptime decl_indexes: anytype,
        comptime index_names: [][]const u8,
        comptime index_keys: [][]const u8,
        comptime index_optss: []IndexOpts,
    ) void {
        comptime {
            if (!isTuple(@TypeOf(decl_indexes))) {
                @compileError("indexes must be tuple, fund:" ++ @typeName(@TypeOf(decl_indexes)));
            }
            for (0..decl_indexes.len) |i| {
                index_names[i] = calcIndexName(table_name, i, decl_indexes[i]);
                index_keys[i] = calcIndexKey(decl_indexes[i]);
                index_optss[i] = decl_indexes[i][1];
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
        const ti_struct = @typeInfo(EntType).Struct;
        const fields = ti_struct.fields;
        const decls = ti_struct.decls;

        const table_name = getLastTypeName(EntType);

        comptime var ent_col_names: [fields.len][]const u8 = undefined;
        comptime var ent_col_types: [fields.len]ValueType = undefined;
        comptime var ent_col_opts: [fields.len]ColOpts = undefined;
        inline for (0..fields.len) |i| {
            ent_col_names[i] = fields[i].name;
            ent_col_types[i] = comptime ValueType.getUndefined(fields[i].type);
            ent_col_opts[i] = comptime calcColOpts(fields[i].type);
        }

        comptime var with_decl_primary_key: bool = false;
        comptime var with_decl_relations: bool = false;
        comptime var with_decl_indexes: bool = false;
        comptime var with_decl_unique_cols: bool = false;
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
                if (std.mem.eql(u8, decls[i].name, "unique_cols")) {
                    with_decl_unique_cols = true;
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
        const foreign_key_capacity = if (with_decl_relations) @field(EntType, "relations").len else 0;
        comptime var foreign_key_names: [foreign_key_capacity][]const u8 = undefined;
        comptime var foreign_key_col_names: [foreign_key_capacity][]const u8 = undefined;
        comptime var foreign_key_target_table_names: [foreign_key_capacity][]const u8 = undefined;
        comptime var foreign_key_target_col_names: [foreign_key_capacity][]const u8 = undefined;
        comptime var foreign_key_relation_optss: [foreign_key_capacity]RelationOpts = undefined;
        const foreign_key_count = if (has_foreign_key)
            calcForeignKeyArrays(
                table_name,
                @field(EntType, "relations"),
                &foreign_key_names,
                &foreign_key_col_names,
                &foreign_key_target_table_names,
                &foreign_key_target_col_names,
                &foreign_key_relation_optss,
            )
        else
            @as(usize, 0);

        const has_indexes = if (with_decl_indexes) @field(EntType, "indexes").len > 0 else false;
        const index_capacity = if (with_decl_indexes) @field(EntType, "indexes").len else 0;
        comptime var index_names: [index_capacity][]const u8 = undefined;
        comptime var index_keys: [index_capacity][]const u8 = undefined;
        comptime var index_optss: [index_capacity]IndexOpts = undefined;
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

        comptime {
            if (with_decl_unique_cols) {
                const decl_unique_cols = @field(EntType, "unique_cols");
                for (decl_unique_cols) |unique_col_name| {
                    var col_idx: usize = 0;
                    var found_col_idx: bool = false;
                    for (0..ent_col_names.len) |i| {
                        if (std.mem.eql(u8, ent_col_names[i], unique_col_name)) {
                            found_col_idx = true;
                            col_idx = i;
                            break;
                        }
                    }
                    if (!found_col_idx) {
                        @compileError("invalid unique col name: not find '" ++ unique_col_name ++ "' in " ++ @typeName(EntType));
                    }
                    ent_col_opts[col_idx].unique = true;
                }
            }
        }

        return TableSchema{
            .table_name = table_name,
            .col_names = &ent_col_names,
            .col_types = &ent_col_types,
            .col_opts = &ent_col_opts,
            .primary_key_names = &primary_key_names,
            .has_foreign_key = has_foreign_key,
            .foreign_key_names = foreign_key_names[0..foreign_key_count],
            .foreign_key_col_names = foreign_key_col_names[0..foreign_key_count],
            .foreign_key_target_table_names = foreign_key_target_table_names[0..foreign_key_count],
            .foreign_key_target_col_names = foreign_key_target_col_names[0..foreign_key_count],
            .foreign_key_relation_optss = foreign_key_relation_optss[0..foreign_key_count],
            .has_indexes = has_indexes,
            .index_names = &index_names,
            .index_keys = &index_keys,
            .index_optss = &index_optss,
        };
    }

    /// check for base table whether they have valid decl of many2many relation etc. After checking, the checked flag will set to true.
    // TODO: what if there is already existing tables?
    pub fn checkRelation(comptime target: *TableSchema, comptime all_tables: []TableSchema) void {
        for (target.foreign_key_target_table_names) |target_table_name| {
            const found: bool = brk: {
                for (all_tables) |table| {
                    if (std.mem.eql(u8, target_table_name, table.table_name)) {
                        break :brk true;
                    }
                }
                break :brk false;
            };
            if (!found) {
                @compileError("'" ++ target.table_name ++ "'' specified a relation to not existing table '" ++ target_table_name ++ "'");
            }
        }
        target.relation_checked = true;
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
        const table1_col_type = brk: {
            for (0..table1.col_names.len) |i| {
                if (std.mem.eql(u8, table1_col_name, table1.col_names[i])) {
                    break :brk table1.col_types[i];
                }
            }
            unreachable;
        };
        const table2_col_type = brk: {
            for (0..table2.col_names.len) |i| {
                if (std.mem.eql(u8, table2_col_name, table2.col_names[i])) {
                    break :brk table2.col_types[i];
                }
            }
            unreachable;
        };

        const col1_name = table1.table_name ++ "_" ++ table1_col_name;
        const col2_name = table2.table_name ++ "_" ++ table2_col_name;

        comptime var ent_col_names: [3][]const u8 = undefined;
        comptime var ent_col_types: [3]ValueType = undefined;
        comptime var ent_col_opts: [3]ColOpts = undefined;
        ent_col_names[0] = "id";
        ent_col_types[0] = ValueType.getUndefined(i64);
        ent_col_opts[0] = ColOpts{};
        ent_col_names[1] = col1_name;
        ent_col_types[1] = table1_col_type;
        ent_col_opts[1] = ColOpts{};
        ent_col_names[2] = col2_name;
        ent_col_types[2] = table2_col_type;
        ent_col_opts[2] = ColOpts{};

        comptime var foreign_key_names: [2][]const u8 = undefined;
        comptime var foreign_key_col_names: [2][]const u8 = undefined;
        comptime var foreign_key_target_table_names: [2][]const u8 = undefined;
        comptime var foreign_key_target_col_names: [2][]const u8 = undefined;
        comptime var foreign_key_relation_optss: [2]RelationOpts = undefined;
        foreign_key_names[0] = "fk_" ++ table_name ++ "_0";
        foreign_key_col_names[0] = col1_name;
        foreign_key_target_table_names[0] = table1.table_name;
        foreign_key_target_col_names[0] = table1_col_name;
        foreign_key_relation_optss[0] = RelationOpts{ .cardinality = .many_to_one };
        foreign_key_names[1] = "fk_" ++ table_name ++ "_1";
        foreign_key_col_names[1] = col2_name;
        foreign_key_target_table_names[1] = table2.table_name;
        foreign_key_target_col_names[1] = table2_col_name;
        foreign_key_relation_optss[1] = RelationOpts{ .cardinality = .many_to_one };

        var primary_key_names = [_][]const u8{"id"};

        return TableSchema{
            .table_name = table_name,
            .col_names = &ent_col_names,
            .col_types = &ent_col_types,
            .col_opts = &ent_col_opts,
            .primary_key_names = &primary_key_names,
            .has_foreign_key = true,
            .foreign_key_names = &foreign_key_names,
            .foreign_key_col_names = &foreign_key_col_names,
            .foreign_key_target_table_names = &foreign_key_target_table_names,
            .foreign_key_target_col_names = &foreign_key_target_col_names,
            .foreign_key_relation_optss = &foreign_key_relation_optss,
            .has_indexes = false,
            .relation_checked = true,
        };
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
            switch (decl_relation[3].cardinality) {
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
        checkEntDef(EntType);
        if (countEntTypeManyToManyRelation(EntType) > 0) {
            @compileError(@typeName(EntType) ++ " defined many_to_many relation, can not genSchemaOne, please use genSchema");
        }
        return comptime calcTableSchema(EntType);
    }

    /// genSchema will auto do relation checking
    pub fn genSchema(comptime ent_type_tuple: anytype) []TableSchema {
        if (!isTuple(@TypeOf(ent_type_tuple))) {
            @compileError("build only accepts a tuple of ent type struct, found:" ++ @typeName(@TypeOf(ent_type_tuple)));
        }

        inline for (0..ent_type_tuple.len) |i| {
            checkEntDef(ent_type_tuple[i]);
        }

        comptime var possible_m2m_table_count: usize = 0;
        inline for (0..ent_type_tuple.len) |i| {
            possible_m2m_table_count += countEntTypeManyToManyRelation(ent_type_tuple[i]);
        }

        comptime var table_schemas: [ent_type_tuple.len + possible_m2m_table_count]TableSchema = undefined;
        comptime var table_schema_gen_count: usize = 0;
        inline for (0..ent_type_tuple.len) |i| {
            table_schemas[i] = comptime calcTableSchema(ent_type_tuple[i]);
            table_schema_gen_count += 1;
        }

        const based_table_schema_count = table_schema_gen_count;
        comptime {
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
        }

        comptime {
            for (0..based_table_schema_count) |i| {
                SchemaUtil.checkRelation(&table_schemas[i], &table_schemas);
            }
        }

        return table_schemas[0..table_schema_gen_count];
    }
};

// constraint

pub const Constraint = struct {};

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
    FLOAT32: f32,
    FLOAT64: f64,
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

// all tests

test "genSchema" {
    const Company = struct {
        id: i64,
        name: []const u8,
        pub const indexes = .{
            .{ [_][]const u8{"name"}, IndexOpts{} },
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

        // has to be pub as if not zig may not compile it
        pub const primary_key = [_][]const u8{"id"};
        pub const relations = .{ .{ "company_id", Company, "id", RelationOpts{} }, .{ "id", Skill, "id", RelationOpts{ .cardinality = .many_to_many } } };
        pub const unique_cols = [_][]const u8{"company_id"};
    };
    {
        const tables = SchemaUtil.genSchema(.{ User, Company, Skill });
        try testing.expectEqual(4, tables.len);

        const user_table = tables[0];
        const company_table = tables[1];
        const skill_table = tables[2];
        const m2m_table = tables[3];

        // user_table

        try testing.expectEqualSlices(u8, "User", user_table.table_name);
        try testing.expectEqual(true, user_table.relation_checked);
        try testing.expectEqualDeep(&[_][]const u8{ "id", "name", "sex", "hobby", "company_id" }, user_table.col_names);
        try testing.expectEqualDeep(&[_]ValueType{
            ValueType{ .INT64 = undefined },
            ValueType{ .TEXT = undefined },
            ValueType{ .BOOL = undefined },
            ValueType{ .TEXT = undefined },
            ValueType{ .INT64 = undefined },
        }, user_table.col_types);

        try testing.expectEqual(5, user_table.col_opts.len);
        try testing.expectEqual(false, user_table.col_opts[0].nullable);
        try testing.expectEqual(true, user_table.col_opts[3].nullable);
        try testing.expectEqual(true, user_table.col_opts[4].unique);

        try testing.expectEqual(true, user_table.has_foreign_key);
        try testing.expectEqualDeep(
            &[_][]const u8{"fk_User_Company_id_o2o"},
            user_table.foreign_key_names,
        );
        try testing.expectEqualDeep(&[_][]const u8{"company_id"}, user_table.foreign_key_col_names);
        try testing.expectEqualDeep(&[_][]const u8{"Company"}, user_table.foreign_key_target_table_names);
        try testing.expectEqualDeep(&[_][]const u8{"id"}, user_table.foreign_key_target_col_names);
        try testing.expectEqualDeep(&[_]RelationOpts{RelationOpts{ .cardinality = .one_to_one }}, user_table.foreign_key_relation_optss);

        try testing.expectEqualDeep(&[_][]const u8{"id"}, user_table.primary_key_names);

        // company_table

        try testing.expectEqualSlices(u8, "Company", company_table.table_name);
        try testing.expectEqual(true, company_table.relation_checked);
        try testing.expectEqualDeep(&[_][]const u8{ "id", "name" }, company_table.col_names);
        try testing.expectEqualDeep(&[_]ValueType{
            ValueType{ .INT64 = undefined },
            ValueType{ .TEXT = undefined },
        }, company_table.col_types);
        try testing.expectEqual(false, company_table.has_foreign_key);
        try testing.expect(company_table.foreign_key_names.len == 0);

        try testing.expectEqual(true, company_table.has_indexes);
        try testing.expectEqualDeep(&[_][]const u8{"idx_Company_name"}, company_table.index_names);
        try testing.expectEqualDeep(&[_][]const u8{"name"}, company_table.index_keys);
        try testing.expectEqualDeep(&[_]IndexOpts{IndexOpts{}}, company_table.index_optss);

        // skill_table

        try testing.expectEqualSlices(u8, "Skill", skill_table.table_name);
        try testing.expectEqual(true, skill_table.relation_checked);

        // m2m_table

        try testing.expectEqualSlices(u8, "m2m_Skill_id_User_id", m2m_table.table_name);
        try testing.expectEqual(true, m2m_table.relation_checked);
        try testing.expectEqualDeep(&[_][]const u8{ "id", "User_id", "Skill_id" }, m2m_table.col_names);
        try testing.expectEqualDeep(&[_]ValueType{
            ValueType{ .INT64 = undefined },
            ValueType{ .INT64 = undefined },
            ValueType{ .INT64 = undefined },
        }, m2m_table.col_types);
        try testing.expectEqual(true, m2m_table.has_foreign_key);
        try testing.expectEqual(false, m2m_table.has_indexes);
        try testing.expectEqualDeep(&[_][]const u8{ "fk_m2m_Skill_id_User_id_0", "fk_m2m_Skill_id_User_id_1" }, m2m_table.foreign_key_names);
        try testing.expectEqualDeep(&[_][]const u8{ "User_id", "Skill_id" }, m2m_table.foreign_key_col_names);
        try testing.expectEqualDeep(&[_][]const u8{ "User", "Skill" }, m2m_table.foreign_key_target_table_names);
        try testing.expectEqualDeep(&[_][]const u8{ "id", "id" }, m2m_table.foreign_key_target_col_names);
        try testing.expectEqualDeep(&[_]RelationOpts{
            RelationOpts{ .cardinality = .many_to_one },
            RelationOpts{ .cardinality = .many_to_one },
        }, m2m_table.foreign_key_relation_optss);
        try testing.expectEqualDeep(&[_][]const u8{"id"}, m2m_table.primary_key_names);
        try testing.expectEqual(false, m2m_table.has_indexes);
    }
}
