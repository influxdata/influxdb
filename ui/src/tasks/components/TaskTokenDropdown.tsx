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
    const {tokens, selectedToken, onTokenChange} = this.props

    return (
      <Dropdown
        selectedID={selectedToken.id}
        buttonColor={ComponentColor.Primary}
        buttonSize={ComponentSize.Small}
        onChange={onTokenChange}
      >
        {tokens.map(t => (
          <Dropdown.Item id={t.id} key={t.id} value={t}>
            {t.description}
          </Dropdown.Item>
        ))}
      </Dropdown>
    )
  }
}
