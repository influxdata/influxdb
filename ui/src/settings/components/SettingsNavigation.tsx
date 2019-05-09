// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Tabs} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'

interface Props {
  tab: string
  orgID: string
}

@ErrorHandling
class SettingsNavigation extends PureComponent<Props> {
  public render() {
    const {tab, orgID} = this.props

    const route = `/orgs/${orgID}`

    return (
      <Tabs.Nav>
        <Tabs.Tab
          title="Members"
          id="members"
          url={`${route}/members`}
          active={'members' === tab}
        />
        <Tabs.Tab
          title="Buckets"
          id="buckets"
          url={`${route}/buckets`}
          active={'buckets' === tab}
        />
        <Tabs.Tab
          title="Telegraf"
          id="telegrafs"
          url={`${route}/telegrafs`}
          active={'telegrafs' === tab}
        />
        <CloudExclude>
          <Tabs.Tab
            title="Scrapers"
            id="scrapers"
            url={`${route}/scrapers`}
            active={'scrapers' === tab}
          />
        </CloudExclude>
        <Tabs.Tab
          title="Variables"
          id="variables"
          url={`${route}/variables`}
          active={'variables' === tab}
        />
        <Tabs.Tab
          title="Templates"
          id="templates"
          url={`${route}/templates`}
          active={'templates' === tab}
        />
        <Tabs.Tab
          title="Labels"
          id="labels"
          url={`${route}/labels`}
          active={'labels' === tab}
        />
        <Tabs.Tab
          title="Tokens"
          id="tokens"
          url={`${route}/tokens`}
          active={'tokens' === tab}
        />
        <Tabs.Tab
          title="Org Profile"
          id="profile"
          url={`${route}/profile`}
          active={'profile' === tab}
        />
      </Tabs.Nav>
    )
  }
}

export default SettingsNavigation
