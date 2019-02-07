// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {ComponentSize, EmptyState, IconFont, Input} from 'src/clockface'
import MemberList from 'src/organizations/components/MemberList'
import FilterList from 'src/shared/components/Filter'

// Types
import {ResourceOwner} from '@influxdata/influx'

// Constants
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'

interface Props {
  members: ResourceOwner[]
  orgName: string
}

interface State {
  searchTerm: string
}

export default class Members extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }
  public render() {
    const {searchTerm} = this.state

    return (
      <>
        <TabbedPageHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter tasks..."
            widthPixels={290}
            value={searchTerm}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterChange}
          />
        </TabbedPageHeader>
        <FilterList<ResourceOwner>
          list={this.props.members}
          searchKeys={['name']}
          searchTerm={searchTerm}
        >
          {ms => <MemberList members={ms} emptyState={this.emptyState} />}
        </FilterList>
      </>
    )
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${orgName} doesn't have any Members , why not invite some?`}
            highlightWords={['Members']}
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Members match your query" />
      </EmptyState>
    )
  }
}
