// Libraries
import {orderBy} from 'lodash'
import React, {PureComponent} from 'react'

// Types
import {Dropdown} from '@influxdata/clockface'
import {Authorization} from 'src/types'

interface Props {
  tokens: Authorization[]
  selectedToken: Authorization
  onTokenChange: (token: Authorization) => void
}

export default class TaskTokenDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        button={(active, onClick) => (
          <Dropdown.Button active={active} onClick={onClick}>
            {this.selectedDescription}
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu onCollapse={onCollapse}>
            {this.dropdownItems}
          </Dropdown.Menu>
        )}
      />
    )
  }
  private get dropdownItems(): JSX.Element[] {
    const {tokens, onTokenChange} = this.props

    if (tokens.length) {
      return orderBy(tokens, [
        ({description}) => description.toLocaleLowerCase(),
      ]).map(t => (
        <Dropdown.Item
          id={t.id}
          key={t.id}
          value={t}
          wrapText={true}
          selected={t.id === this.selectedID}
          onClick={onTokenChange}
        >
          {t.description || 'Name this token'}
        </Dropdown.Item>
      ))
    }
    return [
      <Dropdown.Item
        id="no-tokens"
        key="no-tokens"
        value="no-tokens"
        wrapText={true}
      >
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

  private get selectedDescription(): string {
    const {selectedToken, tokens} = this.props

    if (tokens.length && selectedToken) {
      return selectedToken.description
    }
    return 'Select a Token'
  }
}
