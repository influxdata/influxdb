// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Dropdown} from '@influxdata/clockface'

// Types
import {IconFont, ComponentColor} from '@influxdata/clockface'

interface OwnProps {
  onSelectAllAccess: () => void
  onSelectReadWrite: () => void
}

type Props = OwnProps

export default class GenerateTokenDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        testID="dropdown--gen-token"
        style={{width: '160px'}}
        button={(active, onClick) => (
          <Dropdown.Button
            active={active}
            onClick={onClick}
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            testID="dropdown-button--gen-token"
          >
            Generate
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu onCollapse={onCollapse}>
            {this.optionItems}
          </Dropdown.Menu>
        )}
      />
    )
  }

  private get optionItems(): JSX.Element[] {
    return [
      <Dropdown.Item
        testID="dropdown-item generate-token--read-write"
        id={this.bucketReadWriteOption}
        key={this.bucketReadWriteOption}
        value={this.bucketReadWriteOption}
        onClick={this.handleSelect}
      >
        {this.bucketReadWriteOption}
      </Dropdown.Item>,
      <Dropdown.Item
        testID="dropdown-item generate-token--all-access"
        id={this.allAccessOption}
        key={this.allAccessOption}
        value={this.allAccessOption}
        onClick={this.handleSelect}
      >
        {this.allAccessOption}
      </Dropdown.Item>,
    ]
  }

  private get bucketReadWriteOption(): string {
    return 'Read/Write Token'
  }

  private get allAccessOption(): string {
    return 'All Access Token'
  }

  private handleSelect = (selection: string): void => {
    if (selection === this.allAccessOption) {
      this.props.onSelectAllAccess()
    } else if (selection === this.bucketReadWriteOption) {
      this.props.onSelectReadWrite()
    }
  }
}
