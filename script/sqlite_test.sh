BIN_ENTRY=sqlite_test
LFLAGS="-L./vendor/sqlite3/sqlite-amalgamation-3450200 -lsqlite3 -lc"
ZIG=zig

mkdir -p ./zig-out/bin
${ZIG} test ${LFLAGS} -femit-bin=./zig-out/bin/${BIN_ENTRY} -freference-trace $@
