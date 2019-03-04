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
          url={`${route}/members`}
          active={'members' === tab}
        />
        <Tabs.Tab
          title={'Buckets'}
          id={'buckets'}
          url={`${route}/buckets`}
          active={'buckets' === tab}
        />
        <Tabs.Tab
          title={'Dashboards'}
          id={'dashboards'}
          url={`${route}/dashboards`}
          active={'dashboards' === tab}
        />
        <Tabs.Tab
          title={'Tasks'}
          id={'tasks'}
          url={`${route}/tasks`}
          active={'tasks' === tab}
        />
        <Tabs.Tab
          title={'Telegraf'}
          id={'telegrafs'}
          url={`${route}/telegrafs`}
          active={'telegrafs' === tab}
        />
        <Tabs.Tab
          title={'Scrapers'}
          id={'scrapers'}
          url={`${route}/scrapers`}
          active={'scrapers' === tab}
        />
        <Tabs.Tab
          title={'Variables'}
          id={'variables'}
          url={`${route}/variables`}
          active={'variables' === tab}
        />
      </Tabs.Nav>
    )
  }
}

export default OrganizationNavigation
