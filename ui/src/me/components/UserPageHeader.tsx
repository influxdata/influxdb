// Libraries
import React, {PureComponent} from 'react'

// Components
import {Page} from 'src/pageLayout'

interface Props {
  title: string
}

export default class UserPageHeader extends PureComponent<Props> {
  public render() {
    const {title} = this.props

    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <Page.Title title={title} />
        </Page.Header.Left>
        <Page.Header.Right />
      </Page.Header>
    )
  }
}
