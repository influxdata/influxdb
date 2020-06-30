import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

class LoadDataHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false} testID="load-data--header">
        <Page.Title title="Load Data" />
        <CloudUpgradeButton />
      </Page.Header>
    )
  }
}

export default LoadDataHeader
