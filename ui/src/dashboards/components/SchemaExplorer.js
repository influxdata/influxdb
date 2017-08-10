import React, {PropTypes} from 'react'

import DatabaseList from 'src/data_explorer/components/DatabaseList'
import MeasurementList from 'src/data_explorer/components/MeasurementList'
import FieldList from 'src/data_explorer/components/FieldList'
import QueryEditor from 'src/data_explorer/components/QueryEditor'
import buildInfluxQLQuery from 'utils/influxql'

const TEMPLATE_RANGE = {upper: null, lower: ':dashboardTime:'}

const actionBinder = (id, action) => item => action(id, item)

const rawTextBinder = (links, id, action) => text =>
  action(links.queries, id, text)

const SchemaExplorer = ({
  source: {links},
  query,
  query: {id, range},
  templates,
  actions: {
    chooseTag,
    groupByTag,
    groupByTime,
    chooseNamespace,
    editRawTextAsync,
    chooseMeasurement,
    applyFuncsToField,
    toggleTagAcceptance,
    toggleFieldWithGroupByInterval,
  },
}) =>
  <div className="query-maker--tab-contents">
    <QueryEditor
      query={
        query.rawText ||
        buildInfluxQLQuery(range || TEMPLATE_RANGE, query) ||
        ''
      }
      config={query}
      onUpdate={rawTextBinder(links, id, editRawTextAsync)}
      templates={templates}
    />
    <div className="query-builder">
      <DatabaseList
        query={query}
        onChooseNamespace={actionBinder(id, chooseNamespace)}
      />
      <MeasurementList
        query={query}
        onChooseTag={actionBinder(id, chooseTag)}
        onGroupByTag={actionBinder(id, groupByTag)}
        onChooseMeasurement={actionBinder(id, chooseMeasurement)}
        onToggleTagAcceptance={actionBinder(id, toggleTagAcceptance)}
      />
      <FieldList
        query={query}
        onToggleField={actionBinder(id, toggleFieldWithGroupByInterval)}
        onGroupByTime={actionBinder(id, groupByTime)}
        applyFuncsToField={actionBinder(id, applyFuncsToField)}
      />
    </div>
  </div>

const {arrayOf, func, shape, string} = PropTypes

SchemaExplorer.propTypes = {
  source: shape({
    links: shape({
      queries: string.isRequired,
    }).isRequired,
  }).isRequired,
  query: shape({
    id: string,
  }).isRequired,
  templates: arrayOf(
    shape({
      tempVar: string.isRequired,
    })
  ),
  actions: shape({
    chooseNamespace: func.isRequired,
    chooseMeasurement: func.isRequired,
    applyFuncsToField: func.isRequired,
    chooseTag: func.isRequired,
    groupByTag: func.isRequired,
    toggleField: func.isRequired,
    groupByTime: func.isRequired,
    toggleTagAcceptance: func.isRequired,
    editRawTextAsync: func.isRequired,
  }).isRequired,
}

export default SchemaExplorer
