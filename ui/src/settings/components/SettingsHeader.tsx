import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

class SettingsHeader extends Component {
  public render() {
    return (
      <Page.ControlBar fullWidth={false}>
        <Page.ControlBarLeft>
          <PageTitleWithOrg title="Settings" />
        </Page.ControlBarLeft>
        <Page.ControlBarRight />
      </Page.ControlBar>
    )
  }
}

export default SettingsHeader
