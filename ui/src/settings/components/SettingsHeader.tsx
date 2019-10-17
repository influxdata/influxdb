import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

class SettingsHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.HeaderLeft>
          <PageTitleWithOrg title="Settings" />
        </Page.HeaderLeft>
        <Page.HeaderRight />
      </Page.Header>
    )
  }
}

export default SettingsHeader
