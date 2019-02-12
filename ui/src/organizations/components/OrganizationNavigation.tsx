// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Tabs} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  tab: string
  orgID: string
}

@ErrorHandling
class OrganizationNavigation extends PureComponent<Props> {
  public render() {
    const {tab, orgID} = this.props

    const route = `/organizations/${orgID}`

    return (
      <Tabs.Nav>
        <Tabs.Tab
          title={'Members'}
          id={'members'}
          url={`${route}/members_tab`}
          active={'members_tab' === tab}
        />
        <Tabs.Tab
          title={'Buckets'}
          id={'buckets'}
          url={`${route}/buckets_tab`}
          active={'buckets_tab' === tab}
        />
        <Tabs.Tab
          title={'Dashboards'}
          id={'dashboards'}
          url={`${route}/dashboards_tab`}
          active={'dashboards_tab' === tab}
        />
        <Tabs.Tab
          title={'Tasks'}
          id={'tasks'}
          url={`${route}/tasks_tab`}
          active={'tasks_tab' === tab}
        />
        <Tabs.Tab
          title={'Telegraf'}
          id={'telegrafs'}
          url={`${route}/telegrafs_tab`}
          active={'telegrafs_tab' === tab}
        />
        <Tabs.Tab
          title={'Scrapers'}
          id={'scrapers'}
          url={`${route}/scrapers_tab`}
          active={'scrapers_tab' === tab}
        />
      </Tabs.Nav>
    )
  }
}

export default OrganizationNavigation
