import {AppState} from 'src/types'

const isResourceSelected = resource => resource.shouldInstall

export const getResourceInstallCount = (collection: any[]): number => {
  return collection.filter(isResourceSelected).length
}

export const getTotalResourceCount = (appState: AppState): number => {
  const {activeCommunityTemplate} = appState.resources.templates

  let resourceCount = 0
  Object.keys(activeCommunityTemplate).forEach(resourceType => {
    resourceCount += getResourceInstallCount(
      activeCommunityTemplate[resourceType]
    )
  })

  return resourceCount
}
