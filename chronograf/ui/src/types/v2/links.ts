export interface Links {
  query: string
  dashboards: string
  sources: string
  organization: string
  defaultDashboard: string
  external: {
    statusFeed: string
  }
  flux: {
    ast: string
    self: string
    suggestions: string
  }
}
