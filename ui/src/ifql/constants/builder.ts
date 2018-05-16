export const NEW_FROM = `from(db: "pick a db")\n\t|> filter(fn: (r) => r.tag == "value")\n\t|> range(start: -1m)`
