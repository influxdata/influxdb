export const getNavItemActivation = (
  keywords: string[],
  location: string
): boolean => {
  const ignoreOrgAndOrgID = 3
  const parentPath = location.split('/').slice(ignoreOrgAndOrgID)
  return keywords.some(path => parentPath.includes(path))
}
