import React, {Component} from 'react'

// Components
import {Page} from 'src/pageLayout'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

class LoadDataHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <PageTitleWithOrg title="Load Data" />
        </Page.Header.Left>
        <Page.Header.Right />
      </Page.Header>
    )
  }
}

export default LoadDataHeader
