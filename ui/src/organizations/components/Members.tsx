// Libraries
import React, {PureComponent} from 'react'

// Components
import {ComponentSize, EmptyState} from 'src/clockface'
import MemberList from 'src/organizations/components/MemberList'
import FilterList from 'src/shared/components/Filter'

// Types
import {User} from 'src/api'

interface Props {
  members: User[]
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
      <FilterList<User>
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
