import {produce} from 'immer'
import {Actions, ActionTypes} from 'src/templates/actions/'
import {TemplateSummary, DocumentCreate} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

export interface TemplatesState {
  status: RemoteDataState
  items: TemplateSummary[]
  exportTemplate: {status: RemoteDataState; item: DocumentCreate}
}

const defaultState = (): TemplatesState => ({
  status: RemoteDataState.NotStarted,
  items: [],
  exportTemplate: {status: RemoteDataState.NotStarted, item: null},
})

const templatesReducer = (
  state: TemplatesState = defaultState(),
  action: Actions
): TemplatesState =>
  produce(state, draftState => {
    switch (action.type) {
      case ActionTypes.PopulateTemplateSummaries: {
        const {status, items} = action.payload
        draftState.status = status
        if (items) {
          draftState.items = items
        } else {
          draftState.items = null
        }
        return
      }

      case ActionTypes.SetTemplatesStatus: {
        const {status} = action.payload
        draftState.status = status
        return
      }

      case ActionTypes.SetExportTemplate: {
        const {status, item} = action.payload
        draftState.exportTemplate.status = status
        if (item) {
          draftState.exportTemplate.item = item
        }
        return
      }
    }
  })

export default templatesReducer
