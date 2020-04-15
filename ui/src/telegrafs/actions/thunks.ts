// Libraries
import {normalize} from 'normalizr'
import {Dispatch} from 'react'

// API
import {client} from 'src/utils/api'

// Schemas
import {telegrafSchema, arrayOfTelegrafs} from 'src/schemas/telegrafs'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {
  Action,
  setTelegrafs,
  addTelegraf,
  editTelegraf,
  removeTelegraf,
  setCurrentConfig,
} from 'src/telegrafs/actions/creators'

// Constants
import {
  telegrafGetFailed,
  telegrafCreateFailed,
  telegrafUpdateFailed,
  telegrafDeleteFailed,
  addTelegrafLabelFailed,
  removeTelegrafLabelFailed,
  getTelegrafConfigFailed,
} from 'src/shared/copy/notifications'

// Utils
import {getOrg} from 'src/organizations/selectors'
import {getLabels, getStatus} from 'src/resources/selectors'

// Types
import {ILabel} from '@influxdata/influx'
import {
  AppThunk,
  RemoteDataState,
  GetState,
  Telegraf,
  Label,
  TelegrafEntities,
  ResourceType,
} from 'src/types'

export const getTelegrafs = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const state = getState()
    if (
      getStatus(state, ResourceType.Telegrafs) === RemoteDataState.NotStarted
    ) {
      dispatch(setTelegrafs(RemoteDataState.Loading))
    }
    const org = getOrg(state)

    const telegrafs = await client.telegrafConfigs.getAll(org.id)

    const normTelegrafs = normalize<Telegraf, TelegrafEntities, string[]>(
      telegrafs,
      arrayOfTelegrafs
    )

    dispatch(setTelegrafs(RemoteDataState.Done, normTelegrafs))
  } catch (error) {
    console.error(error)
    dispatch(setTelegrafs(RemoteDataState.Error))
    dispatch(notify(telegrafGetFailed()))
  }
}

export const createTelegraf = (telegraf: Telegraf) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const state = getState()
    const labels = getLabels(state, telegraf.labels)
    const createdTelegraf = await client.telegrafConfigs.create({
      ...telegraf,
      labels,
    })

    const normTelegraf = normalize<Telegraf, TelegrafEntities, string>(
      createdTelegraf,
      telegrafSchema
    )

    dispatch(addTelegraf(normTelegraf))
  } catch (error) {
    console.error(error)
    dispatch(notify(telegrafCreateFailed()))
  }
}

export const updateTelegraf = (telegraf: Telegraf) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const state = getState()
    const labels = getLabels(state, telegraf.labels)
    const t = await client.telegrafConfigs.update(telegraf.id, {
      ...telegraf,
      labels,
    })

    const normTelegraf = normalize<Telegraf, TelegrafEntities, string>(
      t,
      telegrafSchema
    )

    dispatch(editTelegraf(normTelegraf))
  } catch (error) {
    console.error(error)
    dispatch(notify(telegrafUpdateFailed(telegraf.name)))
  }
}

export const deleteTelegraf = (id: string, name: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    await client.telegrafConfigs.delete(id)

    dispatch(removeTelegraf(id))
  } catch (error) {
    console.error(error)
    dispatch(notify(telegrafDeleteFailed(name)))
  }
}

export const addTelegrafLabelsAsync = (
  telegrafID: string,
  labels: Label[]
): AppThunk<Promise<void>> => async (dispatch): Promise<void> => {
  try {
    await client.telegrafConfigs.addLabels(telegrafID, labels as ILabel[])
    const telegraf = await client.telegrafConfigs.get(telegrafID)
    const normTelegraf = normalize<Telegraf, TelegrafEntities, string>(
      telegraf,
      telegrafSchema
    )

    dispatch(editTelegraf(normTelegraf))
  } catch (error) {
    console.error(error)
    dispatch(notify(addTelegrafLabelFailed()))
  }
}

export const removeTelegrafLabelsAsync = (
  telegrafID: string,
  labels: Label[]
): AppThunk<Promise<void>> => async (dispatch): Promise<void> => {
  try {
    await client.telegrafConfigs.removeLabels(telegrafID, labels as ILabel[])
    const telegraf = await client.telegrafConfigs.get(telegrafID)
    const normTelegraf = normalize<Telegraf, TelegrafEntities, string>(
      telegraf,
      telegrafSchema
    )

    dispatch(editTelegraf(normTelegraf))
  } catch (error) {
    console.error(error)
    dispatch(notify(removeTelegrafLabelFailed()))
  }
}

export const getTelegrafConfigToml = (telegrafConfigID: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    dispatch(setCurrentConfig(RemoteDataState.Loading))
    const config = await client.telegrafConfigs.getTOML(telegrafConfigID)
    dispatch(setCurrentConfig(RemoteDataState.Done, config))
  } catch (error) {
    dispatch(setCurrentConfig(RemoteDataState.Error))
    dispatch(notify(getTelegrafConfigFailed()))
  }
}
