import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import OrgHeader from 'src/organizations/containers/OrgHeader'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import OrgTemplateFetcher from 'src/organizations/components/OrgTemplateFetcher'
import OrgTemplatesPage from 'src/organizations/components/OrgTemplatesPage'

// Types
import {Organization, TemplateSummary} from '@influxdata/influx'
import {AppState} from 'src/types/v2'

interface RouterProps {
  params: {
    orgID: string
  }
}

interface StateProps {
  org: Organization
  templates: TemplateSummary[]
}

type Props = WithRouterProps & RouterProps & StateProps

@ErrorHandling
class OrgTemplatesIndex extends Component<Props> {
  public render() {
    const {org, templates} = this.props
    return (
      <>
        <Page titleTag={org.name}>
          <OrgHeader orgID={org.id} />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <Tabs>
                <OrganizationNavigation tab={'templates'} orgID={org.id} />
                <Tabs.TabContents>
                  <TabbedPageSection
                    id="org-view-tab--templates"
                    url="templates"
                    title="Templates"
                  >
                    <OrgTemplateFetcher orgName={org.name}>
                      <OrgTemplatesPage
                        templates={templates}
                        orgName={org.name}
                        orgID={org.id}
                        onImport={this.handleImport}
                      />
                    </OrgTemplateFetcher>
                  </TabbedPageSection>
                </Tabs.TabContents>
              </Tabs>
            </div>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }

  private handleImport = () => {
    const {router, org} = this.props
    router.push(`organizations/${org.id}/templates/import`)
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const {orgs, templates} = state

  const org = orgs.find(o => o.id === props.params.orgID)

  return {
    org,
    templates: templates.items,
  }
}

export default connect<StateProps, {}, {}>(mstp)(
  withRouter<{}>(OrgTemplatesIndex)
)
