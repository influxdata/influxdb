import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'

class LoadDataHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.Title title="Load Data" />
      </Page.Header>
    )
  }
}

export default LoadDataHeader
