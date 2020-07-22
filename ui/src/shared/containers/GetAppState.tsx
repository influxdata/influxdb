// Libraries
import React, {FC, useEffect, useMemo, useState} from 'react'
import {Switch, Route, RouteComponentProps} from 'react-router-dom'
import {useDispatch, useSelector} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState, AppState} from 'src/types'
import NoOrgsPage from 'src/organizations/containers/NoOrgsPage'
import App from 'src/App'

// Actions
import {getMe} from 'src/shared/actions/me'
import {getFlags} from 'src/shared/actions/flags'
import {getOrganizations} from 'src/organizations/actions/thunks'
import {getLinks} from 'src/shared/actions/links'
import RouteToOrg from 'src/shared/containers/RouteToOrg'
import {notify} from 'src/shared/actions/notifications'

// Utils
import {activeFlags} from 'src/shared/selectors/flags'
import {updateReportingContext} from 'src/cloud/utils/reporting'
import {
  getFromLocalStorage,
  removeFromLocalStorage,
  setToLocalStorage,
} from 'src/localStorage'

// Constants
import {sessionTimedOut} from 'src/shared/copy/notifications'
import {CLOUD, CLOUD_SIGNIN_PATHNAME} from 'src/shared/constants'

const FETCH_WAIT = 60000

const GetAppState: FC<RouteComponentProps> = ({history, location}) => {
  const [ignoreMe, setIgnoreMe] = useState(false)
  const dispatch = useDispatch()
  const {orgStatus, meStatus, flagStatus, flags} = useSelector(
    (state: AppState) => {
      return {
        flags: activeFlags(state),
        flagStatus: state.flags.status || RemoteDataState.NotStarted,
        orgStatus: state.resources.orgs.status || RemoteDataState.NotStarted,
        meStatus: ignoreMe ? RemoteDataState.Done : state.me.status,
      }
    }
  )

  const loading = [orgStatus, meStatus, flagStatus].some(
    status =>
      status === RemoteDataState.NotStarted ||
      status === RemoteDataState.Loading
  )
    ? RemoteDataState.Loading
    : RemoteDataState.Done

  useEffect(() => {
    dispatch(getLinks())
  }, [dispatch])

  useEffect(() => {
    if (orgStatus === RemoteDataState.NotStarted) {
      dispatch(getOrganizations())
    }
  }, [dispatch, orgStatus])

  useEffect(() => {
    if (meStatus === RemoteDataState.NotStarted) {
      dispatch(getMe())
      return
    }

    if (meStatus === RemoteDataState.Done) {
      if (!!getFromLocalStorage('redirectTo')) {
        removeFromLocalStorage('redirectTo')
      }

      return
    }

    if (meStatus === RemoteDataState.Error) {
      if (CLOUD) {
        const url = new URL(
          `${window.location.origin}${CLOUD_SIGNIN_PATHNAME}?redirectTo=${window.location.href}`
        )
        setToLocalStorage('redirectTo', window.location.href)
        history.replace(url.href)
        return
      }

      if (location.pathname.startsWith('/signin')) {
        return
      }

      let returnTo = ''

      if (location.pathname !== '/') {
        returnTo = `?returnTo=${location.pathname}`
        dispatch(notify(sessionTimedOut()))
      }

      history.replace(`/signin${returnTo}`)
    }
  }, [dispatch, meStatus])

  useEffect(() => {
    const interval = setInterval(() => {
      // NOTE: after the initial load, this dispatch will update the loading state, which will cause a rerender
      setIgnoreMe(true)
      dispatch(getMe())
    }, FETCH_WAIT)

    return () => {
      clearInterval(interval)
    }
  }, [])

  useEffect(() => {
    if (flagStatus === RemoteDataState.NotStarted) {
      dispatch(getFlags())
    }
  }, [dispatch, flagStatus])

  useEffect(() => {
    updateReportingContext(
      Object.entries(flags).reduce((prev, [key, val]) => {
        prev[`flag (${key})`] = val

        return prev
      }, {})
    )
  }, [flags])

  return useMemo(
    () => (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        <Switch>
          <Route path="/no-orgs" component={NoOrgsPage} />
          <Route path="/orgs" component={App} />
          <Route exact path="/" component={RouteToOrg} />
        </Switch>
      </SpinnerContainer>
    ),
    [loading]
  )
}

export default GetAppState
