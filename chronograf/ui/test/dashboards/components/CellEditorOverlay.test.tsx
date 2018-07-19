import React from 'react'
import {shallow} from 'enzyme'

import CellEditorOverlay from 'src/dashboards/components/CellEditorOverlay'
import QueryMaker from 'src/dashboards/components/QueryMaker'
import {
  source,
  cell,
  timeRange,
  userDefinedTemplateVariables,
  predefinedTemplateVariables,
  thresholdsListColors,
  gaugeColors,
  lineColors,
  query,
} from 'test/fixtures'

jest.mock('src/shared/apis', () => require('mocks/shared/apis'))

const setup = (override = {}) => {
  const props = {
    source,
    sources: [source],
    cell,
    timeRange,
    autoRefresh: 0,
    dashboardID: 9,
    queryStatus: {
      queryID: null,
      status: null,
    },
    templates: [
      ...userDefinedTemplateVariables,
      ...predefinedTemplateVariables,
    ],
    thresholdsListType: 'text',
    thresholdsListColors,
    gaugeColors,
    lineColors,
    editQueryStatus: () => null,
    onCancel: () => {},
    onSave: () => {},
    notify: () => {},
    ...override,
  }

  const wrapper = shallow(<CellEditorOverlay {...props} />)

  return {props, wrapper}
}

describe('Dashboards.Components.CellEditorOverlay', () => {
  describe('rendering', () => {
    describe('if a predefined template variable is used in the query', () => {
      it('should render the query maker with isQuerySupportedByExplorer as false', () => {
        const queryText =
          'SELECT mean(:fields:), mean("usage_user") AS "mean_usage_user" FROM "telegraf"."autogen"."cpu" WHERE time > :dashboardTime: GROUP BY time(:interval:) FILL(null)'
        const {queryConfig} = query
        const updatedQueryConfig = {...queryConfig, rawText: queryText}
        const updatedQueries = [
          {...query, query: queryText, queryConfig: updatedQueryConfig},
        ]
        const updatedCell = {...cell, queries: updatedQueries}
        const {wrapper} = setup({cell: updatedCell})

        const queryMaker = wrapper.find(QueryMaker)
        const activeQuery = queryMaker.prop('activeQuery')

        expect(activeQuery.isQuerySupportedByExplorer).toBe(false)
      })
    })
  })
})
