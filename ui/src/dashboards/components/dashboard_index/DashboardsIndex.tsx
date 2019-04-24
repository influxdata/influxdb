// Libraries
import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import DashboardsIndexContents from 'src/dashboards/components/dashboard_index/DashboardsIndexContents'
import {Page} from 'src/pageLayout'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// Actions
import {
  deleteDashboardAsync,
  updateDashboardAsync,
  createDashboard as createDashboardAction,
  cloneDashboard as cloneDashboardAction,
} from 'src/dashboards/actions'
import {checkDashboardLimits as checkDashboardLimitsAction} from 'src/cloud/actions/limits'

// Types
import {AppState} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'
import {ComponentStatus} from 'src/clockface'

interface DispatchProps {
  handleDeleteDashboard: typeof deleteDashboardAsync
  handleUpdateDashboard: typeof updateDashboardAsync
  checkDashboardLimits: typeof checkDashboardLimitsAction
  createDashboard: typeof createDashboardAction
  cloneDashboard: typeof cloneDashboardAction
}

interface StateProps {
  limitStatus: LimitStatus
}

interface OwnProps {
  router: InjectedRouter
  params: {orgID: string}
}

type Props = DispatchProps & StateProps & OwnProps

interface State {
  searchTerm: string
}

@ErrorHandling
class DashboardIndex extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {
      createDashboard,
      cloneDashboard,
      handleUpdateDashboard,
      handleDeleteDashboard,
    } = this.props
    const {searchTerm} = this.state

    return (
      <>
        <Page titleTag="Dashboards">
          <Page.Header fullWidth={false}>
            <Page.Header.Left>
              <PageTitleWithOrg title="Dashboards" />
            </Page.Header.Left>
            <Page.Header.Right>
              <AddResourceDropdown
                onSelectNew={createDashboard}
                onSelectImport={this.summonImportOverlay}
                onSelectTemplate={this.summonImportFromTemplateOverlay}
                resourceName="Dashboard"
                canImportFromTemplate={true}
                status={this.addResourceStatus}
                titleText={this.addResourceTitleText}
              />
            </Page.Header.Right>
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-md-12">
              <GetResources resource={ResourceTypes.Dashboards}>
                <GetResources resource={ResourceTypes.Labels}>
                  <GetAssetLimits>
                    <DashboardsIndexContents
                      filterComponent={() => (
                        <SearchWidget
                          placeholderText="Filter dashboards..."
                          onSearch={this.handleFilterDashboards}
                          searchTerm={searchTerm}
                        />
                      )}
                      onDeleteDashboard={handleDeleteDashboard}
                      onCreateDashboard={createDashboard}
                      onCloneDashboard={cloneDashboard}
                      onUpdateDashboard={handleUpdateDashboard}
                      searchTerm={searchTerm}
                      onFilterChange={this.handleFilterDashboards}
                      onImportDashboard={this.summonImportOverlay}
                    />
                  </GetAssetLimits>
                </GetResources>
              </GetResources>
            </div>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }

  private handleFilterDashboards = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private summonImportOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/dashboards/import`)
  }

  private summonImportFromTemplateOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/dashboards/import/template`)
  }

  private get addResourceStatus(): ComponentStatus {
    const {limitStatus} = this.props
    if (limitStatus === LimitStatus.EXCEEDED) {
      return ComponentStatus.Disabled
    } else {
      return ComponentStatus.Default
    }
  }

  private get addResourceTitleText(): string {
    const {limitStatus} = this.props
    if (limitStatus === LimitStatus.EXCEEDED) {
      return 'This account has the maximum number of dashboards allowed'
    }
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    cloud: {
      limits: {
        dashboards: {limitStatus},
      },
    },
  } = state

  return {
    limitStatus,
  }
}

const mdtp: DispatchProps = {
  handleDeleteDashboard: deleteDashboardAsync,
  handleUpdateDashboard: updateDashboardAsync,
  checkDashboardLimits: checkDashboardLimitsAction,
  createDashboard: createDashboardAction,
  cloneDashboard: cloneDashboardAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardIndex)
