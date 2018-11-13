import {AppState, Source} from 'src/types/v2'

export const getActiveSource = (state: AppState): Source | null => {
  const {activeSourceID} = state.sources

  if (!activeSourceID) {
    return null
  }

  const source = state.sources.sources[activeSourceID]

  if (!source) {
    return null
  }

  return source
}

export const getSources = (state: AppState): Source[] =>
  Object.values(state.sources.sources)
