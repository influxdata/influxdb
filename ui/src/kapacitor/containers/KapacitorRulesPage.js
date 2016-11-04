import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import {getRules} from 'src/kapacitor/apis';
import {getKapacitor} from 'src/shared/apis';

export const KapacitorRulesPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      type: PropTypes.string.isRequired, // 'influx-enterprise'
      username: PropTypes.string.isRequired,
      links: PropTypes.shape({
        kapacitors: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func,
  },

  getInitialState() {
    return {
      rules: [],
    };
  },

  componentDidMount() {
    getKapacitor(this.props.source).then((kapacitor) => {
      getRules(kapacitor).then(({data: {rules}}) => {
        this.setState({rules});
      });
    });
  },

  render() {
    const {rules} = this.state;
    const {source} = this.props;

    return (
      <div className="kapacitor-rules-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>Kapacitor Rules</h1>
            </div>
          </div>
        </div>
        <div className="container-fluid">
          <div className="panel panel-minimal">
            <div className="panel-heading u-flex u-ai-center u-jc-space-between">
              <h2 className="panel-title">Alert Rules</h2>
            </div>
            <div className="panel-body">
              <table className="table v-center">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Trigger</th>
                    <th>Message</th>
                    <th>Alerts</th>
                  </tr>
                </thead>
                <tbody>
                  {
                    rules.map((rule) => {
                      return (
                        <tr key={rule.id}>
                          <td className="monotype"><Link to={`/sources/${source.id}/alert-rules/${rule.id}`}>{rule.name}</Link></td>
                          <td className="monotype">{rule.trigger}</td>
                          <td className="monotype">{rule.message}</td>
                          <td className="monotype">{rule.alerts.join(', ')}</td>
                        </tr>
                        );
                    })
                  }
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default KapacitorRulesPage;
