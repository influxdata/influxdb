import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'

class SettingsHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.Title title="Settings" />
      </Page.Header>
    )
  }
}

export default SettingsHeader
