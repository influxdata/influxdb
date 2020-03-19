import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'

class OrgHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.Title title="Organization" />
      </Page.Header>
    )
  }
}

export default OrgHeader
