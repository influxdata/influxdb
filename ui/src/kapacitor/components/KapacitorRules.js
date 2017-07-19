import React, {PropTypes} from 'react'
import {Link} from 'react-router'

import NoKapacitorError from 'shared/components/NoKapacitorError'
import SourceIndicator from 'shared/components/SourceIndicator'
import KapacitorRulesTable from 'src/kapacitor/components/KapacitorRulesTable'
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
          <button className="btn btn-primary btn-sm disabled" disabled={true}>
            Create Rule
          </button>
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

  const tableHeader =
    rules.length === 1 ? '1 Alert Rule' : `${rules.length} Alert Rules`
  return (
    <PageContents source={source}>
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <h2 className="panel-title">
          {tableHeader}
        </h2>
        <div className="u-flex u-ai-center u-jc-space-between">
          <Link
            to={`/sources/${source.id}/alert-rules/new`}
            className="btn btn-sm btn-primary"
            style={{marginRight: '4px'}}
          >
            Build Rule
          </Link>
          <Link
            to={`/sources/${source.id}/tickscript/new`}
            className="btn btn-sm btn-info"
          >
            Write TICKscript
          </Link>
        </div>
      </div>
      <KapacitorRulesTable
        source={source}
        rules={rules}
        onDelete={onDelete}
        onChangeRuleStatus={onChangeRuleStatus}
      />
    </PageContents>
  )
}

const PageContents = ({children, source}) =>
  <div className="page">
    <div className="page-header">
      <div className="page-header__container">
        <div className="page-header__left">
          <h1 className="page-header__title">Alert Rules</h1>
        </div>
        <div className="page-header__right">
          <SourceIndicator sourceName={source && source.name} />
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
  source: shape(),
  onCloseTickscript: func,
}

export default KapacitorRules
