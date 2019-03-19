// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {AppState} from 'src/types/v2'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import OrgHeader from 'src/organizations/containers/OrgHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import Collectors from 'src/organizations/components/Collectors'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import GetOrgResources from 'src/organizations/components/GetOrgResources'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import * as notifyActions from 'src/shared/actions/notifications'
import {getOrgTelegrafs} from 'src/telegrafs/actions'

// Types
import {Bucket, Organization, Telegraf} from '@influxdata/influx'
import {client} from 'src/utils/api'
import {RemoteDataState} from 'src/types'

const getBuckets = async (org: Organization) => {
  return client.buckets.getAllByOrg(org.name)
}

interface RouterProps {
  params: {
    orgID: string
  }
}

interface DispatchProps {
  notify: NotificationsActions.PublishNotificationActionCreator
  getOrgTelegrafs: typeof getOrgTelegrafs
}

interface StateProps {
  org: Organization
  telegrafs: Telegraf[]
}

type Props = WithRouterProps & RouterProps & DispatchProps & StateProps

interface State {
  loading: RemoteDataState
}

@ErrorHandling
class OrgTelegrafsIndex extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {loading: RemoteDataState.NotStarted}
  }

  public async componentDidMount() {
    const {org} = this.props

    this.setState({loading: RemoteDataState.Loading})
    try {
      await this.props.getOrgTelegrafs(org)
      this.setState({loading: RemoteDataState.Done})
    } catch (error) {
      //TODO: notify of errors
      this.setState({loading: RemoteDataState.Error})
    }
  }

  public render() {
    const {org, telegrafs} = this.props
    const {loading: loadingTelegrafs} = this.state

    return (
      <Page titleTag={org.name}>
        <OrgHeader orgID={org.id} />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <Tabs>
              <OrganizationNavigation tab={'telegrafs'} orgID={org.id} />
              <Tabs.TabContents>
                <TabbedPageSection
                  id="org-view-tab--telegrafs"
                  url="telegrafs"
                  title="Telegraf"
                >
                  <SpinnerContainer
                    loading={loadingTelegrafs}
                    spinnerComponent={<TechnoSpinner />}
                  >
                    <GetOrgResources<Bucket>
                      organization={org}
                      fetcher={getBuckets}
                    >
                      {(buckets, loadingBuckets) => (
                        <SpinnerContainer
                          loading={loadingBuckets}
                          spinnerComponent={<TechnoSpinner />}
                        >
                          <Collectors
                            collectors={telegrafs}
                            buckets={buckets}
                            orgName={org.name}
                          />
                        </SpinnerContainer>
                      )}
                    </GetOrgResources>
                  </SpinnerContainer>
                </TabbedPageSection>
              </Tabs.TabContents>
            </Tabs>
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

const mstp = (state: AppState, props: WithRouterProps): StateProps => {
  const {
    orgs,
    telegrafs: {list},
  } = state
  const org = orgs.find(o => o.id === props.params.orgID)
  return {
    org,
    telegrafs: list,
  }
}

const mdtp: DispatchProps = {
  notify: notifyActions.notify,
  getOrgTelegrafs,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<{}>(OrgTelegrafsIndex))
