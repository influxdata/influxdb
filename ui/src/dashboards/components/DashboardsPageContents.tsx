import React, {Component, MouseEvent} from 'react'
import {connect} from 'react-redux'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import DashboardsTable from 'src/dashboards/components/DashboardsTable'
import ImportDashboardOverlay from 'src/dashboards/components/ImportDashboardOverlay'
import SearchBar from 'src/hosts/components/SearchBar'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Dashboard} from 'src/types/dashboard'
import {
  showOverlay as showOverlayAction,
  ShowOverlay,
} from 'src/shared/actions/overlayTechnology'
import {OverlayContext} from 'src/shared/components/OverlayTechnology'

interface Props {
  dashboards: Dashboard[]
  onDeleteDashboard: (dashboard: Dashboard) => () => void
  onCreateDashboard: () => void
  onCloneDashboard: (
    dashboard: Dashboard
  ) => (event: MouseEvent<HTMLButtonElement>) => void
  onExportDashboard: (dashboard: Dashboard) => () => void
  onImportDashboard: (dashboard: Dashboard) => void
  showOverlay: ShowOverlay
  dashboardLink: string
}

interface State {
  searchTerm: string
}

@ErrorHandling
class DashboardsPageContents extends Component<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {
      onDeleteDashboard,
      onCreateDashboard,
      onCloneDashboard,
      onExportDashboard,
      dashboardLink,
    } = this.props

    return (
      <FancyScrollbar className="page-contents">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <div className="panel">
                {this.renderPanelHeading}
                <div className="panel-body">
                  <DashboardsTable
                    dashboards={this.filteredDashboards}
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

  private get renderPanelHeading(): JSX.Element {
    const {onCreateDashboard} = this.props

    return (
      <div className="panel-heading">
        <h2 className="panel-title">{this.panelTitle}</h2>
        <div className="panel-controls">
          <SearchBar
            placeholder="Filter by Name..."
            onSearch={this.filterDashboards}
          />
          <Authorized requiredRole={EDITOR_ROLE}>
            <>
              <button
                className="btn btn-sm btn-default"
                onClick={this.showImportOverlay}
              >
                <span className="icon import" /> Import Dashboard
              </button>
              <button
                className="btn btn-sm btn-primary"
                onClick={onCreateDashboard}
              >
                <span className="icon plus" /> Create Dashboard
              </button>
            </>
          </Authorized>
        </div>
      </div>
    )
  }

  private get filteredDashboards(): Dashboard[] {
    const {dashboards} = this.props
    const {searchTerm} = this.state

    return dashboards.filter(d =>
      d.name.toLowerCase().includes(searchTerm.toLowerCase())
    )
  }

  private get panelTitle(): string {
    const {dashboards} = this.props

    if (dashboards === null) {
      return 'Loading Dashboards...'
    } else if (dashboards.length === 1) {
      return '1 Dashboard'
    }

    return `${dashboards.length} Dashboards`
  }

  private filterDashboards = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private showImportOverlay = (): void => {
    const {showOverlay, onImportDashboard} = this.props
    const options = {
      dismissOnClickOutside: false,
      dismissOnEscape: false,
    }

    showOverlay(
      <OverlayContext.Consumer>
        {({onDismissOverlay}) => (
          <ImportDashboardOverlay
            onDismissOverlay={onDismissOverlay}
            onImportDashboard={onImportDashboard}
          />
        )}
      </OverlayContext.Consumer>,
      options
    )
  }
}

const mapDispatchToProps = {
  showOverlay: showOverlayAction,
}

export default connect(null, mapDispatchToProps)(DashboardsPageContents)
