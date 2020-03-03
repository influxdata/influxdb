// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  Page
} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  title: string
  altText?: string
}

@ErrorHandling
class PageTitleWithOrg extends PureComponent<Props> {
  render() {
    const {title, altText} = this.props

    return <Page.Title title={title} altText={altText} />
  }
}

export default PageTitleWithOrg
