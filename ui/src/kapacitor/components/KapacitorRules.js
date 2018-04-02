import React from 'react'
import PropTypes from 'prop-types'
import {Link} from 'react-router'

import NoKapacitorError from 'shared/components/NoKapacitorError'
import KapacitorRulesTable from 'src/kapacitor/components/KapacitorRulesTable'
import TasksTable from 'src/kapacitor/components/TasksTable'

const KapacitorRules = ({
  source,
  rules,
  hasKapacitor,
  loading,
  onDelete,
  onChangeRuleStatus,
}) => {
  if (loading) {
    return (
      <div>
        <div className="panel-heading">
          <h2 className="panel-title">Alert Rules</h2>
          <button className="btn btn-primary btn-sm disabled" disabled={true}>
            Create Rule
          </button>
        </div>
        <div className="panel-body">
          <div className="generic-empty-state">
            {hasKapacitor ? (
              <p>Loading Rules...</p>
            ) : (
              <NoKapacitorError source={source} />
            )}
          </div>
        </div>
      </div>
    )
  }

  const builderRules = rules.filter(r => r.query)

  const builderHeader = `${builderRules.length} Alert Rule${
    builderRules.length === 1 ? '' : 's'
  }`
  const scriptsHeader = `${rules.length} TICKscript${
    rules.length === 1 ? '' : 's'
  }`

  return (
    <div>
      <div className="panel">
        <div className="panel-heading">
          <h2 className="panel-title">{builderHeader}</h2>
          <Link
            to={`/sources/${source.id}/alert-rules/new`}
            className="btn btn-sm btn-primary"
            style={{marginRight: '4px'}}
          >
            <span className="icon plus" /> Build Alert Rule
          </Link>
        </div>
        <div className="panel-body">
          <KapacitorRulesTable
            source={source}
            rules={builderRules}
            onDelete={onDelete}
            onChangeRuleStatus={onChangeRuleStatus}
          />
        </div>
      </div>
      <div className="panel">
        <div className="panel-heading">
          <h2 className="panel-title">{scriptsHeader}</h2>
          <Link
            to={`/sources/${source.id}/tickscript/new`}
            className="btn btn-sm btn-success"
            style={{marginRight: '4px'}}
          >
            <span className="icon plus" /> Write TICKscript
          </Link>
        </div>
        <div className="panel-body">
          <TasksTable
            source={source}
            tasks={rules}
            onDelete={onDelete}
            onChangeRuleStatus={onChangeRuleStatus}
          />
        </div>
      </div>
    </div>
  )
}

const {arrayOf, bool, func, shape} = PropTypes

KapacitorRules.propTypes = {
  source: shape(),
  rules: arrayOf(shape()),
  hasKapacitor: bool,
  loading: bool,
  onChangeRuleStatus: func,
  onDelete: func,
}

export default KapacitorRules
