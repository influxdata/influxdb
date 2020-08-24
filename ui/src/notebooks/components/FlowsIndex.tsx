// Libraries
import React from 'react'

// Components
import {
  Page,
  ResourceList,
  PageHeader,
  Button,
  IconFont,
  ComponentColor,
} from '@influxdata/clockface'
import FlowsIndexEmpty from 'src/notebooks/components/FlowsIndexEmpty'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

const FlowsIndex = () => {
  return (
    <Page titleTag={pageTitleSuffixer(['Flows'])} testID="flows-index">
      <PageHeader fullWidth={false}>
        <Page.Title title="Flows" />
      </PageHeader>
      <Page.ControlBar fullWidth={false}>
        <Page.ControlBarLeft></Page.ControlBarLeft>
        <Page.ControlBarRight>
          <Button
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            text="Create Flow"
            titleText="Click to create a Flow"
            onClick={() => console.log('TODO: create a flow')}
            testID="create-flow--button empty"
          />
        </Page.ControlBarRight>
      </Page.ControlBar>
      <ResourceList>
        <ResourceList.Body emptyState={<FlowsIndexEmpty />}>
          {[]}
        </ResourceList.Body>
      </ResourceList>
    </Page>
  )
}

export default FlowsIndex
