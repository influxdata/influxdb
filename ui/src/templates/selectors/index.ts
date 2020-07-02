import {CommunityTemplate} from 'src/types'

const isResourceSelected = resource => resource.shouldInstall

export const getResourceInstallCount = (collection: any[]): number => {
  return collection.filter(isResourceSelected).length
}

export const getTotalResourceCount = (summary: CommunityTemplate): number => {
  let resourceCount = 0
  Object.keys(summary).forEach(resourceType => {
    resourceCount += getResourceInstallCount(summary[resourceType])
  })

  return resourceCount
}
