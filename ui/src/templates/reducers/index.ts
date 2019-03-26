import {produce} from 'immer'
import {Actions, ActionTypes} from 'src/templates/actions/'
import {TemplateSummary, DocumentCreate} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

export interface TemplatesState {
  status: RemoteDataState
  items: TemplateSummary[]
  exportTemplate: {status: RemoteDataState; item: DocumentCreate; orgID: string}
}

const defaultState = (): TemplatesState => ({
  status: RemoteDataState.NotStarted,
  items: [],
  exportTemplate: {
    status: RemoteDataState.NotStarted,
    item: null,
    orgID: null,
  },
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
        const {status, item, orgID} = action.payload
        draftState.exportTemplate.status = status

        if (item) {
          draftState.exportTemplate.item = item
        } else {
          draftState.exportTemplate.item = null
        }

        if (orgID) {
          draftState.exportTemplate.orgID = orgID
        } else {
          draftState.exportTemplate.orgID = null
        }
        return
      }

      case ActionTypes.RemoveTemplateSummary: {
        const {templateID} = action.payload
        const {items} = draftState
        draftState.items = items.filter(l => {
          return l.id !== templateID
        })

        return
      }
    }
  })

export default templatesReducer
