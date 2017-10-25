import React, {PropTypes} from 'react'
import {Link} from 'react-router'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import NoKapacitorError from 'shared/components/NoKapacitorError'
import SourceIndicator from 'shared/components/SourceIndicator'
import KapacitorRulesTable from 'src/kapacitor/components/KapacitorRulesTable'
import TasksTable from 'src/kapacitor/components/TasksTable'
import FancyScrollbar from 'shared/components/FancyScrollbar'

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
      <PageContents>
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">Alert Rules</h2>
          <Authorized requiredRole={EDITOR_ROLE}>
            <button className="btn btn-primary btn-sm disabled" disabled={true}>
              Create Rule
            </button>
          </Authorized>
        </div>
        <div className="panel-body">
          <div className="generic-empty-state">
            <p>Loading Rules...</p>
          </div>
        </div>
      </PageContents>
    )
  }

  if (!hasKapacitor) {
    return (
      <PageContents>
        <NoKapacitorError source={source} />
      </PageContents>
    )
  }

  const rulez = rules.filter(r => r.query)
  const tasks = rules.filter(r => !r.query)

  const rHeader = `${rulez.length} Alert Rule${rulez.length === 1 ? '' : 's'}`
  const tHeader = `${tasks.length} TICKscript${tasks.length === 1 ? '' : 's'}`

  return (
    <PageContents source={source}>
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <h2 className="panel-title">
          {rHeader}
        </h2>
        <Authorized requiredRole={EDITOR_ROLE}>
          <Link
            to={`/sources/${source.id}/alert-rules/new`}
            className="btn btn-sm btn-primary"
          >
            <span className="icon plus" /> Build Rule
          </Link>
        </Authorized>
      </div>
      <KapacitorRulesTable
        source={source}
        rules={rulez}
        onDelete={onDelete}
        onChangeRuleStatus={onChangeRuleStatus}
      />

      <div className="row">
        <div className="col-md-12">
          <div className="panel panel-minimal">
            <div className="panel-heading u-flex u-ai-center u-jc-space-between">
              <h2 className="panel-title">
                {tHeader}
              </h2>
              <Authorized requiredRole={EDITOR_ROLE}>
                <Link
                  to={`/sources/${source.id}/tickscript/new`}
                  className="btn btn-sm btn-info"
                >
                  Write TICKscript
                </Link>
              </Authorized>
            </div>
            <TasksTable
              source={source}
              tasks={tasks}
              onDelete={onDelete}
              onChangeRuleStatus={onChangeRuleStatus}
            />
          </div>
        </div>
      </div>
    </PageContents>
  )
}

const PageContents = ({children}) =>
  <div className="page">
    <div className="page-header">
      <div className="page-header__container">
        <div className="page-header__left">
          <h1 className="page-header__title">
            Build Alert Rules or Write TICKscripts
          </h1>
        </div>
        <div className="page-header__right">
          <SourceIndicator />
        </div>
      </div>
    </div>
    <FancyScrollbar className="page-contents fancy-scroll--kapacitor">
      <div className="container-fluid">
        <div className="row">
          <div className="col-md-12">
            <div className="panel panel-minimal">
              {children}
            </div>
          </div>
        </div>
      </div>
    </FancyScrollbar>
  </div>

const {arrayOf, bool, func, node, shape} = PropTypes

KapacitorRules.propTypes = {
  source: shape(),
  rules: arrayOf(shape()),
  hasKapacitor: bool,
  loading: bool,
  onChangeRuleStatus: func,
  onDelete: func,
}

PageContents.propTypes = {
  children: node,
  onCloseTickscript: func,
}

export default KapacitorRules
