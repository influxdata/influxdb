export interface Links {
  auths: string
  buckets: string
  signin: string
  signout: string
  dashboards: string
  external: {
    statusFeed: string
  }
  flux: {
    ast: string
    self: string
    suggestions: string
  }
  orgs: string
  query: string
  setup: string
  sources: string
  system: {debug: string; health: string; metrics: string}
  tasks: string
  users: string
  write: string
  defaultDashboard: string
}
