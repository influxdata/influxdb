// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Page,
  FlexBox,
  FlexDirection,
  AlignItems,
  ComponentSize,
} from '@influxdata/clockface'

// Types
import {AppState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  title: string
  altText?: string
}

interface StateProps {
  orgName: string
}

type Props = OwnProps & StateProps

@ErrorHandling
class PageTitleWithOrg extends PureComponent<Props> {
  render() {
    const {orgName, title, altText} = this.props

    return (
      <FlexBox
        direction={FlexDirection.Column}
        alignItems={AlignItems.FlexStart}
        margin={ComponentSize.Small}
      >
        <Page.Title title={title} altText={altText} />
        <Page.SubTitle title={orgName} />
      </FlexBox>
    )
  }
}

const mstp = ({orgs: {org}}: AppState) => {
  return {orgName: org.name}
}

export default connect<StateProps>(mstp)(PageTitleWithOrg)
