// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState, Telegraf, ResourceState, ResourceType} from 'src/types'
import {
  Action,
  SET_TELEGRAFS,
  ADD_TELEGRAF,
  EDIT_TELEGRAF,
  REMOVE_TELEGRAF,
  SET_CURRENT_CONFIG,
} from 'src/telegrafs/actions/creators'

// Utils
import {
  editResource,
  removeResource,
  setResource,
  addResource,
} from 'src/resources/reducers/helpers'

const {Telegrafs} = ResourceType

const initialState = (): TelegrafsState => ({
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
  currentConfig: {status: RemoteDataState.NotStarted, item: ''},
})

type TelegrafsState = ResourceState['telegrafs']

export const telegrafsReducer = (
  state: TelegrafsState = initialState(),
  action: Action
): TelegrafsState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_TELEGRAFS: {
        setResource<Telegraf>(draftState, action, Telegrafs)

        return
      }

      case ADD_TELEGRAF: {
        addResource<Telegraf>(draftState, action, Telegrafs)

        return
      }

      case EDIT_TELEGRAF: {
        editResource<Telegraf>(draftState, action, Telegrafs)

        return
      }

      case REMOVE_TELEGRAF: {
        removeResource<Telegraf>(draftState, action)

        return
      }

      case SET_CURRENT_CONFIG: {
        const {status, item} = action
        draftState.currentConfig.status = status

        if (item) {
          draftState.currentConfig.item = item
        } else {
          draftState.currentConfig.item = ''
        }

        return
      }
    }
  })
