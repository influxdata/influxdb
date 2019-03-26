// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import Table from 'src/dashboards/components/dashboard_index/Table'
import FilterList from 'src/shared/components/Filter'
import GetLabels from 'src/configuration/components/GetLabels'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Dashboard} from 'src/types'
import {Notification} from 'src/types/notifications'

interface Props {
  dashboards: Dashboard[]
  onCreateDashboard: () => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onDeleteDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onFilterChange: (searchTerm: string) => void
  notify: (message: Notification) => void
  searchTerm: string
  showOwnerColumn: boolean
  filterComponent?: () => JSX.Element
  onImportDashboard: () => void
}

@ErrorHandling
export default class DashboardsIndexContents extends Component<Props> {
  public render() {
    const {
      onDeleteDashboard,
      onCloneDashboard,
      onCreateDashboard,
      onUpdateDashboard,
      searchTerm,
      showOwnerColumn,
      dashboards,
      filterComponent,
      onFilterChange,
      onImportDashboard,
    } = this.props

    return (
      <GetLabels>
        <FilterList<Dashboard>
          list={dashboards}
          searchTerm={searchTerm}
          searchKeys={['name', 'labels[].name']}
          sortByKey="name"
        >
          {filteredDashboards => (
            <Table
              filterComponent={filterComponent}
              searchTerm={searchTerm}
              dashboards={filteredDashboards}
              onDeleteDashboard={onDeleteDashboard}
              onCreateDashboard={onCreateDashboard}
              onCloneDashboard={onCloneDashboard}
              onUpdateDashboard={onUpdateDashboard}
              showOwnerColumn={showOwnerColumn}
              onFilterChange={onFilterChange}
              onImportDashboard={onImportDashboard}
            />
          )}
        </FilterList>
      </GetLabels>
    )
  }
}
