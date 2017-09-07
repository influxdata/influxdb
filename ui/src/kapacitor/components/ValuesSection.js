import React, {PropTypes} from 'react'

import _ from 'lodash'

import Deadman from 'src/kapacitor/components/Deadman'
import Threshold from 'src/kapacitor/components/Threshold'
import Relative from 'src/kapacitor/components/Relative'
import DataSection from 'src/kapacitor/components/DataSection'
import RuleGraph from 'src/kapacitor/components/RuleGraph'

import {Tab, TabList, TabPanels, TabPanel, Tabs} from 'shared/components/Tabs'

const TABS = ['Threshold', 'Relative', 'Deadman']

export const ValuesSection = React.createClass({
  propTypes: {
    rule: PropTypes.shape({
      id: PropTypes.string,
    }).isRequired,
    onChooseTrigger: PropTypes.func.isRequired,
    onUpdateValues: PropTypes.func.isRequired,
    query: PropTypes.shape({}).isRequired,
    onDeadmanChange: PropTypes.func.isRequired,
    onRuleTypeDropdownChange: PropTypes.func.isRequired,
    onRuleTypeInputChange: PropTypes.func.isRequired,
    onAddEvery: PropTypes.func.isRequired,
    onRemoveEvery: PropTypes.func.isRequired,
    timeRange: PropTypes.shape({}).isRequired,
    queryConfigActions: PropTypes.shape({}).isRequired,
    source: PropTypes.shape({}).isRequired,
  },

  render() {
    const {
      rule,
      query,
      source,
      timeRange,
      onAddEvery,
      onRemoveEvery,
      onDeadmanChange,
      queryConfigActions,
      onRuleTypeInputChange,
      onRuleTypeDropdownChange,
    } = this.props
    const initialIndex = TABS.indexOf(_.startCase(rule.trigger))

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Rule Conditions</h3>
        <div className="rule-section--body">
          <Tabs initialIndex={initialIndex} onSelect={this.handleChooseTrigger}>
            <TabList isKapacitorTabs="true">
              {TABS.map(tab =>
                <Tab key={tab} isKapacitorTab={true}>
                  {tab}
                </Tab>
              )}
            </TabList>
            <DataSection
              query={query}
              timeRange={timeRange}
              isKapacitorRule={true}
              actions={queryConfigActions}
              onAddEvery={onAddEvery}
              onRemoveEvery={onRemoveEvery}
            />

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
            <RuleGraph
              rule={rule}
              query={query}
              source={source}
              timeRange={timeRange}
            />
          </Tabs>
        </div>
      </div>
    )
  },

  handleChooseTrigger(triggerIndex) {
    const {rule, onChooseTrigger} = this.props
    if (TABS[triggerIndex] === rule.trigger) {
      return
    }

    onChooseTrigger(rule.id, TABS[triggerIndex])
  },

  handleValuesChange(values) {
    const {onUpdateValues, rule} = this.props
    onUpdateValues(rule.id, rule.trigger, values)
  },
})

export default ValuesSection
