import React from 'react';
import FlashMessages from 'shared/components/FlashMessages';

export const SelectSourcePage = React.createClass({

  render() {
    return (
      <div id="select-source-page">
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-summer">
                <div className="panel-heading text-center">
                  <h2 className="deluxe">Welcome to Chronograf</h2>
                </div>
                <div className="panel-body">
                  <form onSubmit={this.handleSubmit}>
                    <div className="form-group col-sm-12">
                      <h4>Select an InfluxDB server to connect to</h4>
                      <label htmlFor="source">InfluxDB Server</label>
                      <input ref={(name) => this.clusterName = name} className="form-control input-lg" type="text" id="source" placeholder="Ex. MyCluster"/>
                    </div>

                    <div className="form-group col-sm-6 col-sm-offset-3">
                      <button className="btn btn-lg btn-block btn-success" type="submit">Connect</button>
                    </div>

                    <div>
                    	<h4>Or connect to a new server</h4>
                    	<label htmlFor="connect-string">connection string</label>
                    	<input id="connect-string" placeholder="http://localhost:8086"></input>
                    	<label htmlFor="user">user</label>
                    	<input id="user"></input>
                    	<label htmlFor="password">password</label>
                    	<input id="password" type="password"></input>
                   	</div>

                    <div className="form-group col-sm-6 col-sm-offset-3">
                      <button className="btn btn-lg btn-block btn-success" type="submit">Create</button>
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default FlashMessages(SelectSourcePage);
