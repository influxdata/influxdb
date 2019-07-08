// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState} from 'src/types'
import {Action} from 'src/labels/actions'
import {Label} from 'src/types'

const initialState = (): LabelsState => ({
  status: RemoteDataState.NotStarted,
  list: [],
})

export interface LabelsState {
  status: RemoteDataState
  list: Label[]
}

export const labelsReducer = (
  state: LabelsState = initialState(),
  action: Action
): LabelsState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_LABELS': {
        const {status, list} = action.payload

        draftState.status = status

        if (list) {
          draftState.list = list
        }

        return
      }

      case 'ADD_LABEL': {
        const {label} = action.payload

        draftState.list.push(label)

        return
      }

      case 'EDIT_LABEL': {
        const {label} = action.payload
        const {list} = draftState

        draftState.list = list.map(l => {
          if (l.id === label.id) {
            return label
          }

          return l
        })

        return
      }

      case 'REMOVE_LABEL': {
        const {id} = action.payload
        const {list} = draftState
        const deleted = list.filter(l => {
          return l.id !== id
        })

        draftState.list = deleted
        return
      }
    }
  })
