import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import FlashMessages from 'shared/components/FlashMessages';
import {getSources} from 'shared/apis';

export const ManageSources = React.createClass({
  propTypes: {
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
    return (
      <div id="select-source-page">
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <table className="table v-center">
                <thead>
                  <tr>
                    <th onClick={this.changeSort} className="sortable-header">Name</th>
                    <th>Host</th>
                    <th>Kapacitor</th>
                    <th>Default</th>
                  </tr>
                </thead>
                <tbody>
                  {
                    sources.map((source) => {
                      return (
                        <tr key={source.id}>
                          <td className="monotype">{source.name}</td>
                          <td className="text-center">{source.url}</td>
                          <td className="monotype">{source.links.kapacitors}</td>
                          <td className="monotype"><input type="checkbox" /></td>
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

export default FlashMessages(withRouter(ManageSources));
