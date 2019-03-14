// Libraries
import {Dispatch} from 'redux'

import {client} from 'src/utils/api'

// Utils
import {addDashboardIDToCells} from 'src/dashboards/apis/'
import {addLabelDefaults} from 'src/shared/utils/labels'

// Actions
import {loadDashboard} from 'src/dashboards/actions/v2/'
import {notify} from 'src/shared/actions/notifications'

// Types
import {Proto} from '@influxdata/influx'
import {GetState, Dashboard} from 'src/types/v2'
import {ConfigurationState} from 'src/types/v2/dataLoaders'

// Const
import {
  TelegrafDashboardFailed,
  TelegrafDashboardCreated,
} from 'src/shared/copy/notifications'

export enum ActionTypes {
  LoadProto = 'LOAD_PROTO',
}

export type Action = LoadProtoAction

interface LoadProtoAction {
  type: ActionTypes.LoadProto
  payload: {
    proto: Proto
  }
}

export const loadProto = (proto: Proto): LoadProtoAction => ({
  type: ActionTypes.LoadProto,
  payload: {proto},
})

export const getProtos = () => async (dispatch: Dispatch<Action>) => {
  try {
    const protos = await client.protos.getAll()

    protos.forEach(p => {
      dispatch(loadProto(p))
    })
  } catch (error) {
    console.error(error)
  }
}

export const createDashFromProto = (
  protoID: string,
  orgID: string
) => async dispatch => {
  try {
    const dashboards = await client.dashboards.createFromProto(protoID, orgID)

    dashboards.forEach((d: Dashboard) => {
      const updatedDashboard = {
        ...d,
        labels: d.labels.map(addLabelDefaults),
        cells: addDashboardIDToCells(d.cells, d.id),
      }
      dispatch(loadDashboard(updatedDashboard))
    })
  } catch (error) {
    console.error(error)
  }
}

export const createDashboardsForPlugins = () => async (
  dispatch,
  getState: GetState
) => {
  await dispatch(getProtos())
  const {
    dataLoading: {
      dataLoaders: {telegrafPlugins},
      steps: {orgID},
    },
    protos,
  } = getState()

  const plugins = []

  try {
    telegrafPlugins.forEach(tp => {
      if (tp.configured === ConfigurationState.Configured) {
        if (protos[tp.name]) {
          dispatch(createDashFromProto(protos[tp.name].id, orgID))
          plugins.push(tp.name)
        }
      }
    })

    if (plugins.length) {
      dispatch(notify(TelegrafDashboardCreated(plugins)))
    }
  } catch (err) {
    console.error(err)
    dispatch(notify(TelegrafDashboardFailed()))
  }
}
