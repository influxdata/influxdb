export const NEW_FROM = `from(db: "pick a db")\n\t|> filter(fn: (r) => r.tag == "value")\n\t|> range(start: -1m)`
export const NEW_JOIN = `join(tables: {table1:table1, table2:table2}, on:["host"], fn: (tables) => tables.table1["_value"] + tables.table2["_value"])`
