import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'

import {createSource, updateSource} from 'shared/apis'
import SourceForm from 'src/sources/components/SourceForm'
import {addSource as addSourceAction} from 'src/shared/actions/sources'

const {
  func,
  shape,
  string,
} = PropTypes

export const CreateSource = React.createClass({
  propTypes: {
    router: shape({
      push: func.isRequired,
    }).isRequired,
    location: shape({
      query: shape({
        redirectPath: string,
      }).isRequired,
    }).isRequired,
    addSourceAction: func,
    updateSourceAction: func,
  },

  getInitialState() {
    return {
      source: {},
    }
  },

  handleNewSource(e) {
    e.preventDefault()
    const source = {
      url: this.sourceURL.value.trim(),
      name: this.sourceName.value,
      username: this.sourceUser.value,
      password: this.sourcePassword.value,
      isDefault: true,
      telegraf: this.sourceTelegraf.value,
      insecureSkipVerify: this.sourceInsecureSkipVerify ? this.sourceInsecureSkipVerify.checked : false,
      metaUrl: this.metaUrl && this.metaUrl.value.trim(),
    }
    createSource(source).then(({data: sourceFromServer}) => {
      this.props.addSourceAction(sourceFromServer)
      this.redirectToApp(sourceFromServer)
    })
  },

  redirectToApp(source) {
    const {redirectPath} = this.props.location.query
    if (!redirectPath) {
      return this.props.router.push(`/sources/${source.id}/hosts`)
    }

    const fixedPath = redirectPath.replace(/\/sources\/[^/]*/, `/sources/${source.id}`)
    return this.props.router.push(fixedPath)
  },

  handleInputChange(e) {
    const val = e.target.value
    const name = e.target.name
    this.setState((prevState) => {
      const newSource = Object.assign({}, prevState.source, {
        [name]: val,
      })
      return Object.assign({}, prevState, {source: newSource})
    })
  },

  handleBlurSourceURL(newSource) {
    if (this.state.editMode) {
      return
    }

    if (!newSource.url) {
      return
    }

    // if there is a type on source it has already been created
    if (newSource.type) {
      return
    }

    createSource(newSource).then(({data: sourceFromServer}) => {
      this.props.addSourceAction(sourceFromServer)
      this.setState({source: sourceFromServer})
    }).catch(({data: error}) => {
      // dont want to flash this until they submit
      this.setState({error: error.message})
    })
  },

  handleSubmit(newSource) {
    const {error} = this.state

    if (error) {
      // useful error message
      // return addFlashMessage({type: 'error', text: error})
    }

    updateSource(newSource).then(({data: sourceFromServer}) => {
      this.props.updateSourceAction(sourceFromServer)
      this.redirectToApp(newSource)
    }).catch(() => {
      // give a useful error message to the user
    })
  },

  // <div className="panel panel-minimal">
  //   <div className="panel-heading text-center">
  //     <h2 className="deluxe">Welcome to Chronograf</h2>
  //   </div>
  //   <div className="panel-body">
  //     <h4 className="text-center">Connect to a New Source</h4>
  //     <br/>
  //
  //     <form onSubmit={this.handleNewSource}>
  //       <div>
  //         <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
  //           <label htmlFor="connect-string">Connection String</label>
  //           <input ref={(r) => this.sourceURL = r} onChange={this.onInputChange} className="form-control" id="connect-string" defaultValue="http://localhost:8086"></input>
  //         </div>
  //         <div className="form-group col-xs-6 col-sm-4">
  //           <label htmlFor="name">Name</label>
  //           <input ref={(r) => this.sourceName = r} className="form-control" id="name" defaultValue="Influx 1"></input>
  //         </div>
  //         <div className="form-group col-xs-6 col-sm-4 col-sm-offset-2">
  //           <label htmlFor="username">Username</label>
  //           <input ref={(r) => this.sourceUser = r} className="form-control" id="username"></input>
  //         </div>
  //         <div className="form-group col-xs-6 col-sm-4">
  //           <label htmlFor="password">Password</label>
  //           <input ref={(r) => this.sourcePassword = r} className="form-control" id="password" type="password"></input>
  //         </div>
  //       </div>
  //       <div className="form-group col-xs-8 col-xs-offset-2">
  //         <label htmlFor="telegraf">Telegraf Database</label>
  //         <input ref={(r) => this.sourceTelegraf = r} className="form-control" id="telegraf" type="text" defaultValue="telegraf"></input>
  //       </div>
  //       {this.state.showSSL ?
  //         <div className="form-group col-xs-8 col-xs-offset-2">
  //           <div className="form-control-static">
  //             <input type="checkbox" id="insecureSkipVerifyCheckbox" ref={(r) => this.sourceInsecureSkipVerify = r} />
  //             <label htmlFor="insecureSkipVerifyCheckbox">Unsafe SSL</label>
  //           </div>
  //           <label className="form-helper">{insecureSkipVerifyText}</label>
  //         </div> : null}
  //       <div className="form-group form-group-submit col-xs-12 text-center">
  //         <button className="btn btn-success" type="submit">Connect New Source</button>
  //       </div>
  //     </form>
  //   </div>
  // </div>

  render() {
    const {source} = this.state

    return (
      <div className="select-source-page" id="select-source-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <SourceForm
                source={source}
                editMode={false}
                onInputChange={this.handleInputChange}
                onSubmit={this.handleSubmit}
                onBlurSourceURL={this.handleBlurSourceURL}
              />
            </div>
          </div>
        </div>
      </div>
    )
  },
})

function mapStateToProps(_) {
  return {}
}

export default connect(mapStateToProps, {addSourceAction})(withRouter(CreateSource))
