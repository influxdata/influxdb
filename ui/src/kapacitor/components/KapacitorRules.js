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
  if (!hasKapacitor) {
    return (
      <PageContents>
        <NoKapacitorError source={source} />
      </PageContents>
    )
  }

  if (loading) {
    return (
      <PageContents>
        <h2>Loading...</h2>
      </PageContents>
    )
  }
  const tableHeader = rules.length === 1
    ? '1 Alert Rule'
    : `${rules.length} Alert Rules`
  return (
    <PageContents source={source}>
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <h2 className="panel-title">{tableHeader}</h2>
        <Link
          to={`/sources/${source.id}/alert-rules/new`}
          className="btn btn-sm btn-primary"
        >
          Create Rule
        </Link>
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

const {arrayOf, bool, func, shape, node} = PropTypes

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
}

export default KapacitorRules
