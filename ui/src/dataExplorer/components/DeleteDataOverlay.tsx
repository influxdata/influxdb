// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {Overlay} from '@influxdata/clockface'
import {get} from 'lodash'

// Components
import DeleteDataForm from 'src/shared/components/DeleteDataForm/DeleteDataForm'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'

// Utils
import {
  getActiveQuery,
  getActiveTimeMachine,
  getTagKeys,
  getTagValues,
} from 'src/timeMachine/selectors'

// Types
import {AppState, TimeRange} from 'src/types'

const resolveTimeRange = (timeRange: TimeRange): [number, number] | null => {
  const [lower, upper] = [
    Date.parse(timeRange.lower),
    Date.parse(timeRange.upper),
  ]

  if (!isNaN(lower) && !isNaN(upper)) {
    return [lower, upper]
  }

  return null
}

interface StateProps {
  selectedBucketName?: string
  selectedKeys: string[]
  selectedTimeRange?: [number, number]
  selectedValues: (string | number)[]
}

const DeleteDataOverlay: FunctionComponent<StateProps & WithRouterProps> = ({
  router,
  params: {orgID},
  selectedBucketName,
  selectedKeys,
  selectedTimeRange,
  selectedValues,
}) => {
  const handleDismiss = () => router.push(`/orgs/${orgID}/data-explorer`)

  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={600}>
        <Overlay.Header title="Delete Data" onDismiss={handleDismiss} />
        <Overlay.Body>
          <GetResources resources={[ResourceType.Buckets]}>
            <DeleteDataForm
              handleDismiss={handleDismiss}
              initialBucketName={selectedBucketName}
              initialTimeRange={selectedTimeRange}
              keys={selectedKeys}
              orgID={orgID}
              values={selectedValues}
            />
          </GetResources>
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => {
  const activeQuery = getActiveQuery(state)
  const selectedBucketName = get(activeQuery, 'builderConfig.buckets.0')
  const selectedBucketTags = get(activeQuery, 'builderConfig.tags')

  const {timeRange} = getActiveTimeMachine(state)
  const selectedTimeRange = resolveTimeRange(timeRange)
  const selectedKeys = getTagKeys(selectedBucketTags)
  const selectedValues = getTagValues(selectedBucketTags)

  return {
    selectedBucketName,
    selectedKeys,
    selectedTimeRange,
    selectedValues,
  }
}

export default connect<StateProps>(mstp)(
  withRouter<StateProps>(DeleteDataOverlay)
)
