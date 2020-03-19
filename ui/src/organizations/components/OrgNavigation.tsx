// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Tabs, Orientation, ComponentSize} from '@influxdata/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Constants
import {CLOUD_USERS_PATH} from 'src/shared/constants'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  activeTab: string
  orgID: string
}

type Props = OwnProps & WithRouterProps

interface OrgPageTab {
  text: string
  id: string
  cloudExclude?: boolean
  cloudOnly?: boolean
  onClick: () => void
  featureFlag?: string
}

@ErrorHandling
class OrgNavigation extends PureComponent<Props> {
  public render() {
    const {activeTab, orgID, router} = this.props

    const tabs: OrgPageTab[] = [
      {
        text: 'Members',
        id: 'members-oss',
        cloudExclude: true,
        onClick: () => {
          router.push(`/orgs/${orgID}/members`)
        },
      },
      {
        text: 'Members',
        id: 'members-cloud',
        featureFlag: 'multiUser',
        cloudOnly: true,
        onClick: () => {
          window.location.assign(`/orgs/${orgID}/${CLOUD_USERS_PATH}`)
        },
      },
      {
        text: 'About',
        id: 'about',
        onClick: () => {
          router.push(`/orgs/${orgID}/about`)
        },
      },
    ]

    return (
      <Tabs orientation={Orientation.Horizontal} size={ComponentSize.Large}>
        {tabs.map(t => {
          let isActive = t.id === activeTab

          if (t.id === 'members-oss' || t.id === 'members-cloud') {
            if (activeTab === 'members') {
              isActive = true
            }
          }

          let tab = (
            <Tabs.Tab
              key={t.id}
              text={t.text}
              id={t.id}
              onClick={t.onClick}
              active={isActive}
            />
          )

          if (t.cloudExclude) {
            tab = <CloudExclude key={t.id}>{tab}</CloudExclude>
          }

          if (t.cloudOnly) {
            tab = <CloudOnly key={t.id}>{tab}</CloudOnly>
          }

          if (t.featureFlag) {
            tab = (
              <FeatureFlag key={t.id} name={t.featureFlag}>
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

export default withRouter(OrgNavigation)
