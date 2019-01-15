// Libraries
import React, {PureComponent} from 'react'

// Components
import {Page} from 'src/pageLayout'

// Constants
import {generateRandomGreeting} from 'src/me/constants'

interface Props {
  userName: string
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
    const {userName} = this.props

    const {text, language} = generateRandomGreeting()

    const title = `${text}, ${userName}!`
    const altText = `That's how you say hello in ${language}`

    return <Page.Title title={title} altText={altText} />
  }
}
