// Libraries
import React, {FC, useEffect} from 'react'
import {useDispatch, useSelector} from 'react-redux'

// Components
import TimeMachine from 'src/timeMachine/components/TimeMachine'
import LimitChecker from 'src/cloud/components/LimitChecker'

// Actions
import {setActiveTimeMachineID} from 'src/timeMachine/actions'
import {executeQueries} from 'src/timeMachine/actions/queries'

// Utils
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import {queryBuilderFetcher} from 'src/timeMachine/apis/QueryBuilderFetcher'
import {getHasQueryText} from 'src/timeMachine/selectors'

const DataExplorer: FC = () => {
  const dispatch = useDispatch()
  const hasQuery = useSelector(getHasQueryText)

  useEffect(() => {
    queryBuilderFetcher.clearCache()
    dispatch(setActiveTimeMachineID('de'))

    if (hasQuery) {
      dispatch(executeQueries())
    }
  }, [dispatch, hasQuery])

  return (
    <LimitChecker>
      <div className="data-explorer">
        <HoverTimeProvider>
          <TimeMachine />
        </HoverTimeProvider>
      </div>
    </LimitChecker>
  )
}

export default DataExplorer
