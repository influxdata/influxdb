import _ from 'lodash'

export const getNavItemActivation = (
  keywords: string[],
  location: string
): boolean => {
  const ignoreOrgAndOrgID = 3
  const parentPath = _.get(location.split('/'), ignoreOrgAndOrgID, '')
  return keywords.some(path => path === parentPath)
}
