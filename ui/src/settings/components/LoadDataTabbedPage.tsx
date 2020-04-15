// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import LoadDataNavigation from 'src/settings/components/LoadDataNavigation'
import {Tabs, Orientation, Page} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  activeTab: string
  orgID: string
}

@ErrorHandling
class LoadDataTabbedPage extends PureComponent<Props> {
  public render() {
    const {activeTab, orgID, children} = this.props

    return (
      <Page.Contents fullWidth={false} scrollable={true}>
        <Tabs.Container orientation={Orientation.Horizontal}>
          <LoadDataNavigation activeTab={activeTab} orgID={orgID} />
          <Tabs.TabContents>{children}</Tabs.TabContents>
        </Tabs.Container>
      </Page.Contents>
    )
  }
}

export default LoadDataTabbedPage
