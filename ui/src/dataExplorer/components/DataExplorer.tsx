// Libraries
import React, {FC, useEffect} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import TimeMachine from 'src/timeMachine/components/TimeMachine'
import LimitChecker from 'src/cloud/components/LimitChecker'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'

// Actions
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {setBuilderBucketIfExists} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import {queryBuilderFetcher} from 'src/timeMachine/apis/QueryBuilderFetcher'
import {readQueryParams} from 'src/shared/utils/queryParams'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const DataExplorer: FC<Props> = ({
  onSetActiveTimeMachine,
  onSetBuilderBucketIfExists,
}) => {
  useEffect(() => {
    const bucketQP = readQueryParams()['bucket']
    onSetActiveTimeMachine('de')
    queryBuilderFetcher.clearCache()
    onSetBuilderBucketIfExists(bucketQP)
  }, [])

  return (
    <LimitChecker>
      <RateLimitAlert />
      <div className="data-explorer">
        <HoverTimeProvider>
          <TimeMachine />
        </HoverTimeProvider>
      </div>
    </LimitChecker>
  )
}

const mdtp = {
  onSetActiveTimeMachine: setActiveTimeMachine,
  onSetBuilderBucketIfExists: setBuilderBucketIfExists,
}

const connector = connect(null, mdtp)

export default connector(DataExplorer)
