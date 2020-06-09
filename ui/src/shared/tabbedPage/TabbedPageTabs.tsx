// Libraries
import React, {FC} from 'react'
import _ from 'lodash'

// Components
import {Tabs, Orientation, ComponentSize} from '@influxdata/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

export interface TabbedPageTab {
  text: string
  id: string
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlagName?: string
  featureFlagValue?: boolean
}

interface Props {
  activeTab: string
  tabs: TabbedPageTab[]
  onTabClick: (id: string) => void
}

const SettingsNavigation: FC<Props> = ({activeTab, tabs, onTabClick}) => {
  return (
    <Tabs orientation={Orientation.Horizontal} size={ComponentSize.Large}>
      {tabs.map(t => {
        let tab = (
          <Tabs.Tab
            testID={`${t.id}--tab`}
            key={t.id}
            text={t.text}
            id={t.id}
            onClick={onTabClick}
            active={t.id === activeTab}
          />
        )

        if (t.cloudExclude) {
          tab = <CloudExclude key={t.id}>{tab}</CloudExclude>
        }

        if (t.cloudOnly) {
          tab = <CloudOnly key={t.id}>{tab}</CloudOnly>
        }

        if (t.featureFlagName) {
          tab = (
            <FeatureFlag
              key={t.id}
              name={t.featureFlagName}
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

export default SettingsNavigation
