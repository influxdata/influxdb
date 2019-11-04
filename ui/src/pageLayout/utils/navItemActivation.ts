// Constants
import {HOMEPAGE_PATHNAME} from 'src/pageLayout/constants/navItemActivation'

export const getNavItemActivation = (
  keywords: string[],
  location: string
): boolean => {
  const ignoreOrgAndOrgID = 3
  const parentPath = location.split('/').slice(ignoreOrgAndOrgID)
  // console.log('location.split()', location.split('/'))
  // console.log('~~ keywords', keywords);
  // console.log('~~ parentPath', parentPath);
  if (!parentPath.length) {
    parentPath.push(HOMEPAGE_PATHNAME)
  }
  return keywords.some(path => parentPath.includes(path))
}
