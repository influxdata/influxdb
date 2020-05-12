// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {Link} from 'react-router'

// Components
import {Tabs, Orientation, ComponentSize} from '@influxdata/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Constants
import {CLOUD_USERS_PATH, CLOUD_URL} from 'src/shared/constants'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  activeTab: string
  orgID: string
}

interface OrgPageTab {
  text: string
  id: string
  cloudExclude?: boolean
  cloudOnly?: boolean
  href?: string
  link?: string
  featureFlag?: string
  featureFlagValue?: boolean
}

@ErrorHandling
class OrgNavigation extends PureComponent<Props> {
  public render() {
    const {activeTab, orgID} = this.props

    const tabs: OrgPageTab[] = [
      {
        text: 'Members',
        id: 'members-oss',
        cloudExclude: true,
        link: `/orgs/${orgID}/members`,
      },
      {
        text: 'Users',
        id: 'members-quartz',
        cloudOnly: true,
        href: `${CLOUD_URL}/organizations/${orgID}${CLOUD_USERS_PATH}`,
        featureFlag: 'multiUser',
      },
      {
        text: 'About',
        id: 'about',
        link: `/orgs/${orgID}/about`,
      },
    ]

    return (
      <Tabs orientation={Orientation.Horizontal} size={ComponentSize.Large}>
        {tabs.map(t => {
          let isActive = t.id === activeTab
          let tab = <></>
          if (t.id === 'members-oss' || t.id === 'members-cloud') {
            if (activeTab === 'members') {
              isActive = true
            }
          }

          if (t.href) {
            tab = (
              <Tabs.Tab
                key={t.id}
                text={t.text}
                id={t.id}
                linkElement={className => (
                  <a href={t.href} className={className} />
                )}
                active={isActive}
              />
            )
          } else if (t.link) {
            tab = (
              <Tabs.Tab
                key={t.id}
                text={t.text}
                id={t.id}
                linkElement={className => (
                  <Link to={t.link} className={className} />
                )}
                active={isActive}
              />
            )
          }

          if (t.cloudExclude) {
            tab = <CloudExclude key={t.id}>{tab}</CloudExclude>
          }

          if (t.cloudOnly) {
            tab = <CloudOnly key={t.id}>{tab}</CloudOnly>
          }

          if (t.featureFlag) {
            tab = (
              <FeatureFlag
                key={t.id}
                name={t.featureFlag}
                equals={t.featureFlagValue}
              >
                {tab}
              </FeatureFlag>
            )
          }

          return tab
        })}
      </Tabs>
    )
  }
}

export default OrgNavigation
