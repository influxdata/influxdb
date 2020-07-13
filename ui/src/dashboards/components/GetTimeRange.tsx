// Libraries
import React, {FC, useEffect} from 'react'
import {useDispatch, useSelector} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {getTimeRange} from 'src/dashboards/selectors'

// Actions
import {
  setDashboardTimeRange,
  updateQueryParams,
} from 'src/dashboards/actions/ranges'

type Props = RouteComponentProps<{dashboardID: string}>

const GetTimeRange: FC<Props> = ({location, match}: Props) => {
  const dispatch = useDispatch()
  const timeRange = useSelector(getTimeRange)
  const isEditing = location.pathname.includes('edit')
  const isNew = location.pathname.includes('new')

  useEffect(() => {
    if (isEditing || isNew) {
      return
    }

    // TODO: map this to current contextID
    dispatch(setDashboardTimeRange(match.params.dashboardID, timeRange))
    const {lower, upper} = timeRange
    dispatch(
      updateQueryParams({
        lower,
        upper,
      })
    )
  }, [dispatch, isEditing, isNew, match.params.dashboardID, timeRange])

  return <div />
}

export default withRouter(GetTimeRange)
