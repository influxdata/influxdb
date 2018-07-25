import {
  getAuthConfig as getAuthConfigAJAX,
  updateAuthConfig as updateAuthConfigAJAX,
} from 'shared/apis/config'

import {errorThrown} from 'shared/actions/errors'

export const getAuthConfigRequested = () => ({
  type: 'CHRONOGRAF_GET_AUTH_CONFIG_REQUESTED',
})

export const getAuthConfigCompleted = authConfig => ({
  type: 'CHRONOGRAF_GET_AUTH_CONFIG_COMPLETED',
  payload: {
    authConfig,
  },
})

export const getAuthConfigFailed = () => ({
  type: 'CHRONOGRAF_GET_AUTH_CONFIG_FAILED',
})

export const updateAuthConfigRequested = authConfig => ({
  type: 'CHRONOGRAF_UPDATE_AUTH_CONFIG_REQUESTED',
  payload: {
    authConfig,
  },
})

export const updateAuthConfigCompleted = () => ({
  type: 'CHRONOGRAF_UPDATE_AUTH_CONFIG_COMPLETED',
})

export const updateAuthConfigFailed = authConfig => ({
  type: 'CHRONOGRAF_UPDATE_AUTH_CONFIG_FAILED',
  payload: {
    authConfig,
  },
})

// async actions (thunks)
export const getAuthConfigAsync = url => async dispatch => {
  dispatch(getAuthConfigRequested())
  try {
    const {data} = await getAuthConfigAJAX(url)
    dispatch(getAuthConfigCompleted(data)) // TODO: change authConfig in actions & reducers to reflect final shape
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(getAuthConfigFailed())
  }
}

export const updateAuthConfigAsync = (
  url,
  oldAuthConfig,
  updatedAuthConfig
) => async dispatch => {
  const newAuthConfig = {...oldAuthConfig, ...updatedAuthConfig}
  dispatch(updateAuthConfigRequested(newAuthConfig))
  try {
    await updateAuthConfigAJAX(url, newAuthConfig)
    dispatch(updateAuthConfigCompleted())
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(updateAuthConfigFailed(oldAuthConfig))
  }
}
