// Libraries
import React, {FC, useEffect, ReactChildren, useState} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Page,
  Button,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import SaveAsButton from 'src/dataExplorer/components/SaveAsButton'
import GetResources from 'src/resources/components/GetResources'
import TimeZoneDropdown from 'src/shared/components/TimeZoneDropdown'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'
import Notebook from 'src/notebooks/components/Notebook'
import LimitChecker from 'src/cloud/components/LimitChecker'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'
import TimeMachineRefreshDropdown from 'src/timeMachine/components/RefreshDropdown'
import SubmitQueryButton from 'src/timeMachine/components/SubmitQueryButton'
import AddVisualizationButton from 'src/notebooks/components/AddVisualizationButton'

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
  const [markdownPanel, setMarkdownPanel] = useState<boolean>(false)

  useEffect(() => {
    const bucketQP = readQueryParams()['bucket']
    onSetActiveTimeMachine('de')
    queryBuilderFetcher.clearCache()
    onSetBuilderBucketIfExists(bucketQP)
  }, [])

  const handleRemoveMarkdownPanel = (): void => {
    setMarkdownPanel(false)
  }

  const handleAddMarkdownPanel = (): void => {
    setMarkdownPanel(true)
  }

  return (
    <Page titleTag={pageTitleSuffixer(['Notebook'])}>
      {children}
      <GetResources resources={[ResourceType.Variables, ResourceType.Buckets]}>
        <Page.Header fullWidth={true}>
          <Page.Title title="Notebook" />
          <CloudUpgradeButton />
        </Page.Header>
        <Page.ControlBar fullWidth={true}>
          <Page.ControlBarLeft>
            <Button
              text="Markdown"
              onClick={handleAddMarkdownPanel}
              color={ComponentColor.Default}
              status={markdownPanel ? ComponentStatus.Disabled : ComponentStatus.Default}
            />
            <Button
              text="Alert"
              color={ComponentColor.Secondary}
              status={ComponentStatus.Disabled}
            />
            <AddVisualizationButton />
            <Button
              text="Downsampler"
              color={ComponentColor.Success}
              status={ComponentStatus.Disabled}
            />
            <Button text="Custom Script" color={ComponentColor.Warning} />
          </Page.ControlBarLeft>
          <Page.ControlBarRight>
            <SubmitQueryButton />
            <TimeMachineRefreshDropdown />
            <TimeZoneDropdown />
            <SaveAsButton />
          </Page.ControlBarRight>
        </Page.ControlBar>
        <Page.Contents fullWidth={true} scrollable={true}>
          <LimitChecker>
            <RateLimitAlert />
            <HoverTimeProvider>
              <Notebook showMarkdownPanel={markdownPanel} onRemoveMarkdownPanel={handleRemoveMarkdownPanel} />
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
