// Libraries
import _ from 'lodash'
import React, {PureComponent} from 'react'

// Types
import {ComponentColor, ComponentSize} from '@influxdata/clockface'
import {Authorization} from '@influxdata/influx'
import {Dropdown} from 'src/clockface'

interface Props {
  tokens: Authorization[]
  selectedToken: Authorization
  onTokenChange: (token: Authorization) => void
}

export default class TaskTokenDropdown extends PureComponent<Props> {
  public render() {
    const {onTokenChange} = this.props

    return (
      <Dropdown
        selectedID={this.selectedID}
        buttonColor={ComponentColor.Primary}
        buttonSize={ComponentSize.Small}
        onChange={onTokenChange}
      >
        {this.dropdownTokens}
      </Dropdown>
    )
  }
  private get dropdownTokens(): JSX.Element[] {
    const {tokens} = this.props
    if (tokens.length) {
      return tokens.map(t => (
        <Dropdown.Item id={t.id} key={t.id} value={t}>
          {t.description || 'Name this token'}
        </Dropdown.Item>
      ))
    }
    return [
      <Dropdown.Item id="no-tokens" key="no-tokens" value="no-tokens">
        You don’t have any tokens with appropriate permissions for this use
      </Dropdown.Item>,
    ]
  }

  private get selectedID(): string {
    const {selectedToken, tokens} = this.props

    if (tokens.length && selectedToken) {
      return selectedToken.id
    }
    return 'no-tokens'
  }
}
