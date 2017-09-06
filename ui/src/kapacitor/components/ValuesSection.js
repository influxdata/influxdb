import React, {PropTypes} from 'react'

import _ from 'lodash'

import Deadman from 'src/kapacitor/components/Deadman'
import Threshold from 'src/kapacitor/components/Threshold'
import Relative from 'src/kapacitor/components/Relative'

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
    onThresholdInputChange: PropTypes.func.isRequired,
  },

  render() {
    const {
      rule,
      query,
      onDeadmanChange,
      onRuleTypeDropdownChange,
      onThresholdInputChange,
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

            <TabPanels>
              <TabPanel>
                <Threshold
                  rule={rule}
                  query={query}
                  onDropdownChange={onRuleTypeDropdownChange}
                  onThresholdInputChange={onThresholdInputChange}
                />
              </TabPanel>
              <TabPanel>
                <Relative rule={rule} onChange={this.handleValuesChange} />
              </TabPanel>
              <TabPanel>
                <Deadman rule={rule} onChange={onDeadmanChange} />
              </TabPanel>
            </TabPanels>
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
