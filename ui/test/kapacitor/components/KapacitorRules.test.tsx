import React from 'react'
import {shallow} from 'enzyme'

import _ from 'lodash'

import KapacitorRules from 'src/kapacitor/components/KapacitorRules'
import KapacitorRulesTable from 'src/kapacitor/components/KapacitorRulesTable'
import TasksTable from 'src/kapacitor/components/TasksTable'

import {source, kapacitorRules} from 'test/resources'

describe('Kapacitor.Containers.KapacitorRules', () => {
  const props = {
    source,
    rules: kapacitorRules,
    hasKapacitor: true,
    loading: false,
    onDelete: () => {},
    onChangeRuleStatus: () => {},
  }

  describe('rendering', () => {
    it('renders KapacitorRules', () => {
      const wrapper = shallow(<KapacitorRules {...props} />)

      expect(wrapper.exists()).toBe(true)
    })

    it('renders KapacitorRulesTable', () => {
      const wrapper = shallow(<KapacitorRules {...props} />)

      const kapacitorRulesTable = wrapper.find(KapacitorRulesTable)
      expect(kapacitorRulesTable.length).toEqual(1)

      const tasksTable = wrapper.find(TasksTable)
      expect(tasksTable.length).toEqual(1)
    })

    it('renders TasksTable', () => {
      const wrapper = shallow(<KapacitorRules {...props} />)

      const tasksTable = wrapper.find(TasksTable)
      expect(tasksTable.length).toEqual(1)
    })

    describe('rows in KapacitorRulesTable and TasksTable', () => {
      const findRows = (root, reactTable) =>
        root
          .find(reactTable)
          .dive()
          .find('tbody')
          .children()
          .map(child => {
            const ruleID = child.key()
            const elRow = child.dive()
            const elLabel = elRow.find('label')
            const {htmlFor} = elLabel.props()
            const elCheckbox = elRow.find({type: 'checkbox'})
            const {checked, id} = elCheckbox.props()

            return {
              row: {
                el: elRow,
                label: {
                  el: elLabel,
                  htmlFor,
                },
                checkbox: {
                  el: elCheckbox,
                  checked,
                  id,
                },
              },
              rule: {
                id: ruleID, // rule.id
              },
            }
          })

      const containsAnyDuplicate = arr => _.uniq(arr).length !== arr.length

      let wrapper, rulesRows, tasksRows

      beforeEach(() => {
        wrapper = shallow(<KapacitorRules {...props} />)
        rulesRows = findRows(wrapper, KapacitorRulesTable)
        tasksRows = findRows(wrapper, TasksTable)
      })

      it('renders every rule/task checkbox with unique html id', () => {
        const allCheckboxIDs = rulesRows
          .map(r => r.row.checkbox.id)
          .concat(tasksRows.map(r => r.row.checkbox.id))

        expect(containsAnyDuplicate(allCheckboxIDs)).toEqual(false)
      })

      it('renders each rule/task table row label with unique "for" attribute', () => {
        const allCheckboxLabelFors = rulesRows
          .map(r => r.row.label.htmlFor)
          .concat(tasksRows.map(r => r.row.label.htmlFor))

        expect(containsAnyDuplicate(allCheckboxLabelFors)).toEqual(false)
      })

      it('renders one corresponding task row for each rule row', () => {
        expect(rulesRows.length).toBeLessThanOrEqual(tasksRows.length)

        rulesRows.forEach(ruleRow => {
          expect(
            tasksRows.filter(taskRow => taskRow.rule.id === ruleRow.rule.id)
              .length
          ).toEqual(1)
        })
      })

      it('renders corresponding rule/task rows with the same enabled status', () => {
        const correspondingRows = []

        rulesRows.forEach(ruleRow => {
          let taskRow = tasksRows
            .filter(t => t.rule.id === ruleRow.rule.id)
            .pop()
          if (taskRow) {
            correspondingRows.push({ruleRow, taskRow})
          }
        })

        correspondingRows.forEach(({ruleRow, taskRow}) => {
          expect(ruleRow.row.checkbox.checked).toEqual(
            taskRow.row.checkbox.checked
          )
        })
      })
    })
  })
})
