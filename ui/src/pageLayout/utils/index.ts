// Constants
import {HOMEPAGE_PATHNAME} from 'src/shared/constants'

export const getNavItemActivation = (
  keywords: string[],
  location: string
): boolean => {
  const ignoreOrgAndOrgID = 3
  const parentPath = location.split('/').slice(ignoreOrgAndOrgID)
  if (!parentPath.length) {
    parentPath.push(HOMEPAGE_PATHNAME)
  }
  return keywords.some(path => parentPath.includes(path))
}
