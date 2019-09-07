/*
  Given a route relative to an org, returns an absolute route including the
  current org.

  For example,

      formatOrgRoute('/alerting/history')

  would return

      '/orgs/someID/alerting/history'

*/
export const formatOrgRoute = (route: string): string => {
  return (
    location.pathname
      .split('/')
      .slice(0, 3)
      .join('/') + route
  )
}
