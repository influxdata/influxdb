export const isLightMode = (
  dashboardLightMode: boolean,
  pathname: string,
  search: string
): boolean => {
  return (
    pathname.includes('dashboards') &&
    search.includes('?lower=') &&
    dashboardLightMode
  )
}
