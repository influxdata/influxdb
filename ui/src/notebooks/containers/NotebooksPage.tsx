// Libraries
import React, {FC, useEffect, ReactChildren} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from '@influxdata/clockface'
import SaveAsButton from 'src/dataExplorer/components/SaveAsButton'
import GetResources from 'src/resources/components/GetResources'
import TimeZoneDropdown from 'src/shared/components/TimeZoneDropdown'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'
import Notebook from 'src/notebooks/components/Notebook'
import LimitChecker from 'src/cloud/components/LimitChecker'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'

// Actions
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {setBuilderBucketIfExists} from 'src/timeMachine/actions/queryBuilder'

// Types
import {ResourceType} from 'src/types'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import {queryBuilderFetcher} from 'src/timeMachine/apis/QueryBuilderFetcher'
import {readQueryParams} from 'src/shared/utils/queryParams'

interface DispatchProps {
  onSetActiveTimeMachine: typeof setActiveTimeMachine
  onSetBuilderBucketIfExists: typeof setBuilderBucketIfExists
}

interface OwnProps {
  children: ReactChildren
}

type Props = OwnProps & DispatchProps

const NotebooksPage: FC<Props> = ({
  children,
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
    <Page titleTag={pageTitleSuffixer(['Notebook'])}>
      {children}
      <GetResources resources={[ResourceType.Variables, ResourceType.Buckets]}>
        <Page.Header fullWidth={true}>
          <Page.Title title="Notebook" />
          <CloudUpgradeButton />
        </Page.Header>
        <Page.ControlBar fullWidth={true}>
          <Page.ControlBarLeft />
          <Page.ControlBarRight>
            <TimeZoneDropdown />
            <SaveAsButton />
          </Page.ControlBarRight>
        </Page.ControlBar>
        <Page.Contents fullWidth={true} scrollable={true}>
          <LimitChecker>
            <RateLimitAlert />
            <HoverTimeProvider>
              <Notebook />
            </HoverTimeProvider>
          </LimitChecker>
        </Page.Contents>
      </GetResources>
    </Page>
  )
}

const mdtp: DispatchProps = {
  onSetActiveTimeMachine: setActiveTimeMachine,
  onSetBuilderBucketIfExists: setBuilderBucketIfExists,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(NotebooksPage)
