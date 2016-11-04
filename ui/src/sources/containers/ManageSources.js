import React, {PropTypes} from 'react';
import _ from 'lodash';
import {withRouter, Link} from 'react-router';
import {getSources, getKapacitor} from 'shared/apis';

export const ManageSources = React.createClass({
  propTypes: {
    location: PropTypes.shape({
      pathname: PropTypes.string.isRequired,
    }).isRequired,
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
        self: PropTypes.string.isRequired,
      }),
    }),
  },
  getInitialState() {
    return {
      sources: [],
    };
  },

  componentDidMount() {
    getSources().then(({data: {sources}}) => {
      this.setState({sources}, () => {
        sources.forEach((source) => {
          getKapacitor(source).then((kapacitor) => {
            this.setState((prevState) => {
              const newSources = prevState.sources.map((newSource) => {
                if (newSource.id !== source.id) {
                  return newSource;
                }

                return Object.assign({}, newSource, {kapacitor});
              });

              return Object.assign({}, prevState, {sources: newSources});
            });
          });
        });
      });
    });
  },

  render() {
    const {sources} = this.state;
    const {pathname} = this.props.location;
    const numSources = sources.length;
    const sourcesTitle = `${numSources} ${numSources === 1 ? 'Source' : 'Sources'}`;

    return (
      <div id="manage-sources-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>InfluxDB Sources</h1>
            </div>
          </div>
        </div>
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">

              <div className="panel panel-minimal">
                <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                  <h2 className="panel-title">{sourcesTitle}</h2>
                  <Link to={`/sources/${this.props.source.id}/manage-sources/new`} className="btn btn-sm btn-primary">Add New Source</Link>
                </div>
                <div className="panel-body">
                  <div className="table-responsive margin-bottom-zero">
                    <table className="table v-center margin-bottom-zero">
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Host</th>
                          <th>Kapacitor</th>
                          <th className="text-right"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {
                          sources.map((source) => {
                            return (
                              <tr key={source.id}>
                                <td>{source.name}{source.default ? <span className="label label-primary">Default</span> : null}</td>
                                <td>{source.url}</td>
                                <td>{_.get(source, ['kapacitor', 'name'], '')}</td>
                                <td className="text-right">
                                  <Link className="btn btn-default btn-xs" to={`${pathname}/${source.id}/edit`}>Edit</Link>
                                  <Link className="btn btn-success btn-xs" to={`/sources/${source.id}/hosts`}>Connect</Link>
                                </td>
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
          </div>
        </div>
      </div>
    );
  },
});

export default withRouter(ManageSources);
