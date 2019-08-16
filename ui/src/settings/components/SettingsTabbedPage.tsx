// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import SettingsNavigation from 'src/settings/components/SettingsNavigation'
import {
  Tabs,
  Orientation,
  ComponentSize,
  InfluxColors,
} from '@influxdata/clockface'
import {Page} from 'src/pageLayout'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  activeTab: string
  orgID: string
}

@ErrorHandling
class SettingsTabbedPage extends PureComponent<Props> {
  public render() {
    const {activeTab, orgID, children} = this.props

    return (
      <Page.Contents fullWidth={false} scrollable={true}>
        <div className="col-xs-12">
          <Tabs.Container
            orientation={Orientation.Vertical}
            className="tabs tabbed-page"
          >
            <SettingsNavigation activeTab={activeTab} orgID={orgID} />
            <Tabs.TabContents
              padding={ComponentSize.Large}
              backgroundColor={InfluxColors.Castle}
            >
              {children}
            </Tabs.TabContents>
          </Tabs.Container>
        </div>
      </Page.Contents>
    )
  }
}

export default SettingsTabbedPage
