import React, {PropTypes} from 'react';
import {withRouter, Link} from 'react-router';
import FlashMessages from 'shared/components/FlashMessages';
import {getSources} from 'shared/apis';

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
      this.setState({sources});
    });
  },

  changeSort() {},

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
                    <th onClick={this.changeSort} className="sortable-header">Name</th>
                    <th>Host</th>
                    <th>Kapacitor</th>
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
                          <td>{source.links.kapacitors}</td>
                          <td><Link to={`${pathname}/${source.id}/edit`}>Edit</Link></td>
                        </tr>
                        );
                    })
                  }
                </tbody>
              </table>
              <div className="btn btn-success">Connect</div>
              <div className="btn btn-primary">Add</div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default FlashMessages(withRouter(ManageSources));
