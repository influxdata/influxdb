import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import SettingsNavigation from 'src/settings/components/SettingsNavigation'
import SettingsHeader from 'src/settings/components/SettingsHeader'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import TemplatesPage from 'src/templates/components/TemplatesPage'

// Types
import {Organization} from '@influxdata/influx'
import {AppState} from 'src/types'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

interface StateProps {
  org: Organization
}

type Props = WithRouterProps & StateProps

@ErrorHandling
class TemplatesIndex extends Component<Props> {
  public render() {
    const {org} = this.props
    return (
      <>
        <Page titleTag={org.name}>
          <SettingsHeader />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <Tabs>
                <SettingsNavigation tab="templates" orgID={org.id} />
                <Tabs.TabContents>
                  <TabbedPageSection
                    id="org-view-tab--templates"
                    url="templates"
                    title="Templates"
                  >
                    <GetResources resource={ResourceTypes.Templates}>
                      <TemplatesPage onImport={this.handleImport} />
                    </GetResources>
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
    router.push(`/orgs/${org.id}/templates/import`)
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    orgs: {org},
  } = state

  return {
    org,
  }
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(withRouter<{}>(TemplatesIndex))
