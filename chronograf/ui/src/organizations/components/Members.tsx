// Libraries
import React, {PureComponent} from 'react'

// Components
import {ComponentSize, EmptyState} from 'src/clockface'
import MemberList from 'src/organizations/components/MemberList'
import FilterList from 'src/organizations/components/Filter'

// Types
import {Member} from 'src/types/v2'

interface Props {
  members: Member[]
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
    const {members} = this.props
    const {searchTerm} = this.state

    return (
      <FilterList<Member>
        list={members}
        searchKeys={['name']}
        searchTerm={searchTerm}
      >
        {ms => <MemberList members={ms} emptyState={this.emptyState} />}
      </FilterList>
    )
  }

  private get emptyState(): JSX.Element {
    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="This org has been abandoned" />
      </EmptyState>
    )
  }
}
