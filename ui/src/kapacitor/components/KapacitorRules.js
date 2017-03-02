import React, {PropTypes} from 'react';
import NoKapacitorError from '../../shared/components/NoKapacitorError';
import {Link} from 'react-router';

const KapacitorRules = ({
  source,
  rules,
  hasKapacitor,
  loading,
  onDelete,
  onChangeRuleStatus,
}) => {
  if (loading) {
    return <Loading />
  }

  if (!hasKapacitor) {
    return <KapacitorError source={source} />
  }

  return (
    <PageContents>
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">Alert Rules</h2>
          <Link to={`/sources/${source.id}/alert-rules/new`} className="btn btn-sm btn-primary">Create New Rule</Link>
        </div>
        <KapacitorRuleTable
          source={source}
          rules={rules}
          onDelete={onDelete}
          onChangeRuleStatus={onChangeRuleStatus}
        />
      </div>
    </PageContents>
  )
}

const KapacitorRuleTable = ({source, rules, onDelete, onChangeRuleStatus}) => {
  return (
    <div className="panel-body">
      <table className="table v-center">
        <thead>
          <tr>
            <th>Name</th>
            <th>Trigger</th>
            <th>Message</th>
            <th>Alerts</th>
            <th className="text-center">Enabled</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {
            rules.map((rule) => {
              return <RuleRow key={rule.id} rule={rule} source={source} onDelete={onDelete} onChangeRuleStatus={onChangeRuleStatus} />
            })
          }
        </tbody>
      </table>
    </div>
  )
}

const RuleRow = ({rule, source, onDelete, onChangeRuleStatus}) => {
  return (
    <tr key={rule.id}>
      <td className="monotype"><Link to={`/sources/${source.id}/alert-rules/${rule.id}`}>{rule.name}</Link></td>
      <td className="monotype">{rule.trigger}</td>
      <td className="monotype">{rule.message}</td>
      <td className="monotype">{rule.alerts.join(', ')}</td>
      <td className="monotype text-center">
        <div className="dark-checkbox">
          <input
            id={`kapacitor-enabled ${rule.id}`}
            className="form-control-static"
            type="checkbox"
            defaultChecked={rule.status === "enabled"}
            onClick={() => onChangeRuleStatus(rule)}
          />
          <label htmlFor={`kapacitor-enabled ${rule.id}`}></label>
        </div>
      </td>
      <td className="text-right"><button className="btn btn-danger btn-xs" onClick={() => onDelete(rule)}>Delete</button></td>
    </tr>
  )
}

const Header = () => (
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1>Kapacitor Rules</h1>
      </div>
    </div>
  </div>
)

const Loading = () => (
  <PageContents>
    <h1>Loading...</h1>
  </PageContents>
)

const KapacitorError = ({source}) => (
  <PageContents>
    <NoKapacitorError source={source} />;
  </PageContents>
)

const PageContents = ({children}) => (
  <div className="page">
    <Header />
    <div className="page-contents">
      <div className="container-fluid">
        <div className="row">
          <div className="col-md-12">
            {children}
          </div>
        </div>
      </div>
    </div>
  </div>
)

const {
  arrayOf,
  bool,
  func,
  shape,
} = PropTypes

KapacitorRules.propTypes = {
  source: shape(),
  rules: arrayOf(shape()),
  hasKapacitor: bool,
  loading: bool,
  onChangeRuleStatus: func,
  onDelete: func,
}

KapacitorRuleTable.propTypes = {
  source: shape(),
  rules: arrayOf(shape()),
  onChangeRuleStatus: func,
  onDelete: func,
}

RuleRow.propTypes = {
  rule: shape(),
  source: shape(),
  onChangeRuleStatus: func,
  onDelete: func,
}

KapacitorError.propTypes = {
  source: shape(),
}

export default KapacitorRules
