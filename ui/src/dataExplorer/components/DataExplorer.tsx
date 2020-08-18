// Libraries
import React, {FC, useEffect} from 'react'
import {useDispatch, useSelector} from 'react-redux'

// Components
import TimeMachine from 'src/timeMachine/components/TimeMachine'
import LimitChecker from 'src/cloud/components/LimitChecker'

// Actions
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {setBuilderBucketIfExists} from 'src/timeMachine/actions/queryBuilder'
import {saveAndExecuteQueries} from 'src/timeMachine/actions/queries'

// Utils
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import {queryBuilderFetcher} from 'src/timeMachine/apis/QueryBuilderFetcher'
import {readQueryParams} from 'src/shared/utils/queryParams'
import {getHasQueryText} from 'src/timeMachine/selectors'

const DataExplorer: FC = () => {
  const dispatch = useDispatch()
  const hasQueryText = useSelector(getHasQueryText)

  useEffect(() => {
    const bucketQP = readQueryParams()['bucket']
    dispatch(setActiveTimeMachine('de'))
    queryBuilderFetcher.clearCache()
    dispatch(setBuilderBucketIfExists(bucketQP))
  }, [dispatch])

  useEffect(() => {
    if (hasQueryText) {
      dispatch(saveAndExecuteQueries)
    }
  }, [dispatch, hasQueryText])

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
