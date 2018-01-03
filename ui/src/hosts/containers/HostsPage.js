import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

import HostsTable from 'src/hosts/components/HostsTable'
import SourceIndicator from 'shared/components/SourceIndicator'

import {getCpuAndLoadForHosts, getLayouts, getAppsForHosts} from '../apis'
import {getEnv} from 'src/shared/apis/env'

class HostsPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      hosts: {},
      hostsLoading: true,
      hostsError: '',
    }
  }

  async componentDidMount() {
    const {source, links, addFlashMessage} = this.props

    const {telegrafSystemInterval} = await getEnv(links.environment)

    const hostsError = 'Unable to get apps for hosts'
    let hosts, layouts

    try {
      const [h, {data}] = await Promise.all([
        getCpuAndLoadForHosts(
          source.links.proxy,
          source.telegraf,
          telegrafSystemInterval
        ),
        getLayouts(),
        new Promise(resolve => {
          this.setState({hostsLoading: true})
          resolve()
        }),
      ])

      hosts = h
      layouts = data.layouts

      this.setState({
        hosts,
        hostsLoading: false,
      })
    } catch (error) {
      this.setState({
        hostsError: error.toString(),
        hostsLoading: false,
      })

      console.error(error)
    }

    if (!hosts || !layouts) {
      addFlashMessage({type: 'error', text: hostsError})
      return this.setState({
        hostsError,
        hostsLoading: false,
      })
    }

    try {
      const newHosts = await getAppsForHosts(
        source.links.proxy,
        hosts,
        layouts,
        source.telegraf
      )
      this.setState({
        hosts: newHosts,
        hostsError: '',
        hostsLoading: false,
      })
    } catch (error) {
      console.error(error)
      addFlashMessage({type: 'error', text: hostsError})
      this.setState({
        hostsError,
        hostsLoading: false,
      })
    }
  }

  render() {
    const {source} = this.props
    const {hosts, hostsLoading, hostsError} = this.state
    return (
      <div className="page hosts-list-page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Host List</h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator />
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-12">
                <HostsTable
                  source={source}
                  hosts={_.values(hosts)}
                  hostsLoading={hostsLoading}
                  hostsError={hostsError}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

const mapStateToProps = ({links}) => {
  return {
    links,
  }
}

HostsPage.propTypes = {
  source: shape({
    id: string.isRequired,
    name: string.isRequired,
    type: string, // 'influx-enterprise'
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
    telegraf: string.isRequired,
  }),
  links: shape({
    environment: string.isRequired,
  }),
  addFlashMessage: func,
}

export default connect(mapStateToProps, null)(HostsPage)
