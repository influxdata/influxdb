import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'

class SettingsHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.Title title="Settings" testID="settings-page--header" />
        <RateLimitAlert />
      </Page.Header>
    )
  }
}

export default SettingsHeader
