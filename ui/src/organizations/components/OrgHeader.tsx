import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

class OrgHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.Title title="Organization" />
        <CloudUpgradeButton />
      </Page.Header>
    )
  }
}

export default OrgHeader
