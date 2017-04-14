import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import {getKapacitor} from 'shared/apis'
import {removeAndLoadSources} from 'src/shared/actions/sources'
import {connect} from 'react-redux'

import InfluxTable from '../components/InfluxTable'

const {
  array,
  func,
  shape,
  string,
} = PropTypes

export const ManageSources = React.createClass({
  propTypes: {
    location: shape({
      pathname: string.isRequired,
    }).isRequired,
    source: shape({
      id: string.isRequired,
      links: shape({
        proxy: string.isRequired,
        self: string.isRequired,
      }),
    }),
    sources: array,
    addFlashMessage: func,
    removeAndLoadSources: func,
  },

  getInitialState() {
    return {
      kapacitors: {},
    }
  },

  componentDidMount() {
    const updates = []
    const kapas = {}

    this.props.sources.forEach((source) => {
      const prom = getKapacitor(source).then((kapacitor) => {
        kapas[source.id] = kapacitor
      })
      updates.push(prom)
    })
    Promise.all(updates).then(() => {
      this.setState({kapacitors: kapas})
    })
  },

  handleDeleteSource(source) {
    const {addFlashMessage} = this.props

    try {
      this.props.removeAndLoadSources(source)
    } catch (e) {
      addFlashMessage({type: 'error', text: 'Could not remove source from Chronograf'})
    }
  },

  render() {
    // probably should move kapacitors to props and use redux store
    const {kapacitors} = this.state
    const {sources, source, location} = this.props

    return (
      <div className="page" id="manage-sources-page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>Configuration</h1>
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <InfluxTable
              handleDeleteSource={this.handleDeleteSource}
              source={source}
              sources={sources}
              kapacitors={kapacitors}
              location={location}
            />
          </div>
        </div>
      </div>
    )
  },
})

function mapStateToProps(state) {
  return {
    sources: state.sources,
  }
}

export default connect(mapStateToProps, {removeAndLoadSources})(withRouter(ManageSources))
