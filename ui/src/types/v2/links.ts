// Links is the response from the /api/v2 endpoint.  It contains
// links to all other endpoints.
// see https://github.com/influxdata/influxdb/blob/db5b20f4eabcc7d2233e54415cbd48945b0b4d0c/http/api_handler.go#L125
export interface Links {
  authorizations: string
  buckets: string
  dashboards: string
  external: {
    statusFeed: string
  }
  query: {
    self: string
    ast: string
    spec: string
    suggestions: string
  }
  orgs: string
  setup: string
  signin: string
  signout: string
  sources: string
  system: {debug: string; health: string; metrics: string}
  tasks: string
  write: string
  users: string
  macros: string
  views: string
  me: string
  defaultDashboard: string
}
