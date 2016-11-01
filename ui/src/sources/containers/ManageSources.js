import React, {PropTypes} from 'react';
import _ from 'lodash';
import {withRouter, Link} from 'react-router';
import {getSources, getKapacitor} from 'shared/apis';

export const ManageSources = React.createClass({
  propTypes: {
    location: PropTypes.shape({
      pathname: PropTypes.string.isRequired,
    }).isRequired,
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

    return (
      <div id="manage-sources-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                Manage Sources
              </h1>
            </div>
          </div>
        </div>
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <table className="table v-center">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Host</th>
                    <th>Kapacitor</th>
                    <th></th>
                    <th></th>
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
                          <td><Link className="btn btn-default btn-xs" to={`${pathname}/${source.id}/edit`}>Edit</Link></td>
                          <td><Link className="btn btn-success btn-xs" to={`/sources/${source.id}/hosts`}>Connect</Link></td>
                        </tr>
                      );
                    })
                  }
                </tbody>
              </table>
              <Link to={`/sources/1/manage-sources/new`} className="btn btn-primary">Add</Link>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default withRouter(ManageSources);
