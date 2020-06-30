// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'

// Components
import {Dropdown} from '@influxdata/clockface'

// Types
import {IconFont, ComponentColor} from '@influxdata/clockface'

type Props = WithRouterProps

class GenerateTokenDropdown extends PureComponent<Props> {
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
      this.handleAllAccess()
    } else if (selection === this.bucketReadWriteOption) {
      this.handleReadWrite()
    }
  }

  private handleAllAccess = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/load-data/tokens/generate/all-access`)
  }

  private handleReadWrite = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/load-data/tokens/generate/buckets`)
  }
}

export default withRouter<{}>(GenerateTokenDropdown)
