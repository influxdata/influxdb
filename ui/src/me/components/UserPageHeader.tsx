// Libraries
import React, {PureComponent} from 'react'

// Components
import {Page} from '@influxdata/clockface'

// Constants
import {generateRandomGreeting} from 'src/me/constants'

interface Props {
  userName: string
  orgName: string
}

export default class UserPageHeader extends PureComponent<Props> {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>{this.title}</Page.Header.Left>
        <Page.Header.Right />
      </Page.Header>
    )
  }

  private get title(): JSX.Element {
    const {userName, orgName} = this.props

    const {text, language} = generateRandomGreeting()

    let title = ''

    if (process.env.CLOUD === 'true') {
      title = `${text}, ${userName}! Welcome to InfluxDB Cloud!`
    } else {
      title = `${text}, ${userName}! Welcome to ${orgName}!`
    }

    const altText = `That's how you say hello in ${language}`

    return <Page.Title title={title} altText={altText} />
  }
}
