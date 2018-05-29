import React, {Component} from 'react'
import PropTypes from 'prop-types'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import DashboardsTable from 'src/dashboards/components/DashboardsTable'
import SearchBar from 'src/hosts/components/SearchBar'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class DashboardsPageContents extends Component {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  filterDashboards = searchTerm => {
    this.setState({searchTerm})
  }

  render() {
    const {
      dashboards,
      onDeleteDashboard,
      onCreateDashboard,
      onCloneDashboard,
      onExportDashboard,
      dashboardLink,
    } = this.props
    const {searchTerm} = this.state

    let tableHeader
    if (dashboards === null) {
      tableHeader = 'Loading Dashboards...'
    } else if (dashboards.length === 1) {
      tableHeader = '1 Dashboard'
    } else {
      tableHeader = `${dashboards.length} Dashboards`
    }
    const filteredDashboards = dashboards.filter(d =>
      d.name.toLowerCase().includes(searchTerm.toLowerCase())
    )

    return (
      <FancyScrollbar className="page-contents">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <div className="panel">
                <div className="panel-heading">
                  <h2 className="panel-title">{tableHeader}</h2>
                  <div className="dashboards-page--actions">
                    <SearchBar
                      placeholder="Filter by Name..."
                      onSearch={this.filterDashboards}
                    />
                    <Authorized requiredRole={EDITOR_ROLE}>
                      <button
                        className="btn btn-sm btn-primary"
                        onClick={onCreateDashboard}
                      >
                        <span className="icon plus" /> Create Dashboard
                      </button>
                    </Authorized>
                  </div>
                </div>
                <div className="panel-body">
                  <DashboardsTable
                    dashboards={filteredDashboards}
                    onDeleteDashboard={onDeleteDashboard}
                    onCreateDashboard={onCreateDashboard}
                    onCloneDashboard={onCloneDashboard}
                    onExportDashboard={onExportDashboard}
                    dashboardLink={dashboardLink}
                  />
                </div>
              </div>
            </div>
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

DashboardsPageContents.propTypes = {
  dashboards: arrayOf(shape()),
  onDeleteDashboard: func.isRequired,
  onCreateDashboard: func.isRequired,
  onCloneDashboard: func.isRequired,
  onExportDashboard: func.isRequired,
  dashboardLink: string.isRequired,
}

export default DashboardsPageContents
