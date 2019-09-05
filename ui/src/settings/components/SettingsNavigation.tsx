// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {withRouter, WithRouterProps} from 'react-router'
import chroma from 'chroma-js'

// Components
import {
  Tabs,
  Orientation,
  ComponentSize,
  InfluxColors,
} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'

interface OwnProps {
  activeTab: string
  orgID: string
}

type Props = OwnProps & WithRouterProps

@ErrorHandling
class SettingsNavigation extends PureComponent<Props> {
  public render() {
    const {activeTab, orgID, router} = this.props

    const handleTabClick = (id: string): void => {
      router.push(`/orgs/${orgID}/settings/${id}`)
    }

    const tabs = [
      {
        text: 'Members',
        id: 'members',
        cloudExclude: false,
      },
      {
        text: 'Variables',
        id: 'variables',
        cloudExclude: false,
      },
      {
        text: 'Templates',
        id: 'templates',
        cloudExclude: false,
      },
      {
        text: 'Labels',
        id: 'labels',
        cloudExclude: false,
      },
      {
        text: 'Org Profile',
        id: 'profile',
        cloudExclude: false,
      },
    ]

    return (
      <Tabs
        orientation={Orientation.Horizontal}
        padding={ComponentSize.Large}
        backgroundColor={`${chroma(`${InfluxColors.Castle}`).alpha(0.1)}`}
      >
        {tabs.map(t => {
          if (t.cloudExclude) {
            return (
              <CloudExclude key={t.id}>
                <Tabs.Tab
                  text={t.text}
                  id={t.id}
                  onClick={handleTabClick}
                  active={t.id === activeTab}
                  size={ComponentSize.Large}
                  backgroundColor={InfluxColors.Castle}
                />
              </CloudExclude>
            )
          }
          return (
            <Tabs.Tab
              key={t.id}
              text={t.text}
              id={t.id}
              onClick={handleTabClick}
              active={t.id === activeTab}
              size={ComponentSize.Large}
              backgroundColor={InfluxColors.Castle}
            />
          )
        })}
      </Tabs>
    )
  }
}

export default withRouter(SettingsNavigation)
