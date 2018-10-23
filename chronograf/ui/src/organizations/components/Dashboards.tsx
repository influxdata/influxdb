// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import ProfilePage from 'src/shared/components/profile_page/ProfilePage'
import {ComponentSize, EmptyState, Input, IconFont} from 'src/clockface'
import FilterList from 'src/organizations/components/Filter'
import DashboardList from 'src/organizations/components/DashboardList'

// Types
import {Dashboard} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  dashboards: Dashboard[]
}

interface State {
  searchTerm: string
}

@ErrorHandling
export default class Dashboards extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {searchTerm} = this.state
    const {dashboards} = this.props

    return (
      <>
        <ProfilePage.Header>
          <Input
            icon={IconFont.Search}
            widthPixels={290}
            value={searchTerm}
            onBlur={this.handleFilterBlur}
            onChange={this.handleFilterChange}
            placeholder="Filter Dashboards..."
          />
        </ProfilePage.Header>
        <FilterList<Dashboard>
          searchTerm={searchTerm}
          searchKeys={['name']}
          list={dashboards}
        >
          {ds => <DashboardList dashboards={ds} emptyState={this.emptyState} />}
        </FilterList>
      </>
    )
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private get emptyState(): JSX.Element {
    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="Oh noes I dun see na dashbardsss" />
      </EmptyState>
    )
  }
}
