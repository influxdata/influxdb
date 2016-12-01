import React, {PropTypes} from 'react';
import _ from 'lodash';
import {withRouter, Link} from 'react-router';
import {getSources, getKapacitor, deleteSource} from 'shared/apis';

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
    addFlashMessage: PropTypes.func,
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

  handleDeleteSource(source) {
    const {addFlashMessage} = this.props;

    deleteSource(source).then(() => {
      const updatedSourceList = this.state.sources.filter((s) => s.id !== source.id);
      this.setState({sources: updatedSourceList});
      addFlashMessage({type: 'success', text: 'Source removed from Chronograf'});
    }).catch(() => {
      addFlashMessage({type: 'error', text: 'Could not remove source from Chronograf'});
    });
  },

  render() {
    const {sources} = this.state;
    const {pathname} = this.props.location;
    const numSources = sources.length;
    const sourcesTitle = `${numSources} ${numSources === 1 ? 'Source' : 'Sources'}`;

    return (
      <div className="page" id="manage-sources-page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>InfluxDB Sources</h1>
            </div>
          </div>
        </div>
        <div className="page-contents">
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
                                  <td>{source.name}{source.default ? <span className="default-source-label">Default</span> : null}</td>
                                  <td>{source.url}</td>
                                  <td>{_.get(source, ['kapacitor', 'name'], '')}</td>
                                  <td className="text-right">
                                    <Link className="btn btn-info btn-xs" to={`${pathname}/${source.id}/edit`}><span className="icon pencil"></span></Link>
                                    <Link className="btn btn-success btn-xs" to={`/sources/${source.id}/hosts`}>Connect</Link>
                                    <button className="btn btn-danger btn-xs" onClick={() => this.handleDeleteSource(source)}><span className="icon trash"></span></button>
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
      </div>
    );
  },
});

export default withRouter(ManageSources);
