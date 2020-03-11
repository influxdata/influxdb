export const isLightMode = (
  dashboardLightMode: boolean,
  pathname: string,
  search: string
): boolean => {
  const fullURL = pathname + search
  const regex = /.+orgs[/][\w\d]+[/]dashboards[/][\w\d]+[?/].+/g
  const keyword = 'isADashboard'
  const urlIsADashboard = fullURL.replace(regex, keyword) === keyword

  return urlIsADashboard && dashboardLightMode
}
