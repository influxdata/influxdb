import React, {PropTypes, Component} from 'react'

import DashboardsTable from 'src/dashboards/components/DashboardsTable'
import SearchBar from 'src/hosts/components/SearchBar'
import FancyScrollbar from 'shared/components/FancyScrollbar'

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
      dashboardLink,
    } = this.props

    let tableHeader
    if (dashboards === null) {
      tableHeader = 'Loading Dashboards...'
    } else if (dashboards.length === 1) {
      tableHeader = '1 Dashboard'
    } else {
      tableHeader = `${dashboards.length} Dashboards`
    }

    const filteredDashboards = dashboards.filter(d =>
      d.name.includes(this.state.searchTerm)
    )

    return (
      <FancyScrollbar className="page-contents">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <div className="panel panel-minimal">
                <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                  <h2 className="panel-title">
                    {tableHeader}
                  </h2>
                  <div className="u-flex u-ai-center dashboards-page--actions">
                    <SearchBar
                      placeholder="Filter by Name..."
                      onSearch={this.filterDashboards}
                    />
                    <button
                      className="btn btn-sm btn-primary"
                      onClick={onCreateDashboard}
                    >
                      <span className="icon plus" /> Create Dashboard
                    </button>
                  </div>
                </div>
                <div className="panel-body">
                  <DashboardsTable
                    dashboards={filteredDashboards}
                    onDeleteDashboard={onDeleteDashboard}
                    onCreateDashboard={onCreateDashboard}
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
  dashboardLink: string.isRequired,
}

export default DashboardsPageContents
