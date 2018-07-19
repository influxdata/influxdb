import React, {SFC, ChangeEvent} from 'react'
import _ from 'lodash'

import Deadman from 'src/kapacitor/components/Deadman'
import Threshold from 'src/kapacitor/components/Threshold'
import Relative from 'src/kapacitor/components/Relative'
import DataSection from 'src/kapacitor/components/DataSection'
import RuleGraph from 'src/kapacitor/components/RuleGraph'

import {
  Tab,
  TabList,
  TabPanels,
  TabPanel,
  Tabs,
} from 'src/shared/components/Tabs'

import {AlertRule, QueryConfig, Source, TimeRange} from 'src/types'
import {KapacitorQueryConfigActions} from 'src/types/actions'

const TABS = ['Threshold', 'Relative', 'Deadman']

const handleChooseTrigger = (rule, onChooseTrigger) => triggerIndex => {
  if (TABS[triggerIndex] === rule.trigger) {
    return
  }
  return onChooseTrigger(rule.id, TABS[triggerIndex])
}
const initialIndex = rule => TABS.indexOf(_.startCase(rule.trigger))
const isDeadman = rule => rule.trigger === 'deadman'

interface Item {
  text: string
}

interface TypeItem extends Item {
  type: string
}

interface Props {
  rule: AlertRule
  onChooseTrigger: () => void
  onUpdateValues: () => void
  query: QueryConfig
  onDeadmanChange: (item: Item) => void
  onRuleTypeDropdownChange: (item: TypeItem) => void
  onRuleTypeInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onAddEvery: (frequency: string) => void
  onRemoveEvery: () => void
  timeRange: TimeRange
  queryConfigActions: KapacitorQueryConfigActions
  source: Source
  onChooseTimeRange: (timeRange: TimeRange) => void
}

const ValuesSection: SFC<Props> = ({
  rule,
  query,
  source,
  timeRange,
  onAddEvery,
  onChooseTrigger,
  onDeadmanChange,
  onChooseTimeRange,
  queryConfigActions,
  onRuleTypeInputChange,
  onRuleTypeDropdownChange,
}) => (
  <div className="rule-section">
    <h3 className="rule-section--heading">Alert Type</h3>
    <div className="rule-section--body">
      <Tabs
        initialIndex={initialIndex(rule)}
        onSelect={handleChooseTrigger(rule, onChooseTrigger)}
      >
        <TabList isKapacitorTabs="true">
          {TABS.map(tab => (
            <Tab key={tab} isKapacitorTab={true}>
              {tab}
            </Tab>
          ))}
        </TabList>
        <div>
          <h3 className="rule-section--sub-heading">Time Series</h3>
          <DataSection
            query={query}
            timeRange={timeRange}
            isKapacitorRule={true}
            actions={queryConfigActions}
            onAddEvery={onAddEvery}
            isDeadman={isDeadman(rule)}
          />
        </div>
        <h3 className="rule-section--sub-heading">Conditions</h3>
        <TabPanels>
          <TabPanel>
            <Threshold
              rule={rule}
              query={query}
              onDropdownChange={onRuleTypeDropdownChange}
              onRuleTypeInputChange={onRuleTypeInputChange}
            />
          </TabPanel>
          <TabPanel>
            <Relative
              rule={rule}
              onDropdownChange={onRuleTypeDropdownChange}
              onRuleTypeInputChange={onRuleTypeInputChange}
            />
          </TabPanel>
          <TabPanel>
            <Deadman rule={rule} onChange={onDeadmanChange} />
          </TabPanel>
        </TabPanels>
        {isDeadman(rule) ? null : (
          <RuleGraph
            rule={rule}
            query={query}
            source={source}
            timeRange={timeRange}
            onChooseTimeRange={onChooseTimeRange}
          />
        )}
      </Tabs>
    </div>
  </div>
)

export default ValuesSection
