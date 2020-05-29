// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState, ResourceState, Label, ResourceType} from 'src/types'
import {
  Action,
  SET_LABELS,
  SET_LABEL,
  REMOVE_LABEL,
} from 'src/labels/actions/creators'

// Utils
import {
  setResource,
  setResourceAtID,
  removeResource,
} from 'src/resources/reducers/helpers'

type LabelsState = ResourceState['labels']

export const initialState = (): LabelsState => ({
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
})

export const labelsReducer = (
  state: LabelsState = initialState(),
  action: Action
): LabelsState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_LABELS: {
        setResource<Label>(draftState, action, ResourceType.Labels)

        return
      }

      case SET_LABEL: {
        setResourceAtID<Label>(draftState, action, ResourceType.Labels)

        return
      }

      case REMOVE_LABEL: {
        removeResource<Label>(draftState, action)

        return
      }
    }
  })
