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

    Promise.all([
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
      .then(([hosts, {data: {layouts}}]) => {
        this.setState({
          hosts,
          hostsLoading: false,
        })
        getAppsForHosts(source.links.proxy, hosts, layouts, source.telegraf)
          .then(newHosts => {
            this.setState({
              hosts: newHosts,
              hostsError: '',
              hostsLoading: false,
            })
          })
          .catch(error => {
            console.error(error)
            const reason = 'Unable to get apps for hosts'
            addFlashMessage({type: 'error', text: reason})
            this.setState({
              hostsError: reason,
              hostsLoading: false,
            })
          })
      })
      .catch(reason => {
        this.setState({
          hostsError: reason.toString(),
          hostsLoading: false,
        })
        // TODO: this isn't reachable at the moment, because getCpuAndLoadForHosts doesn't fail when it should.
        // (like with a bogus proxy link). We should provide better messaging to the user in this catch after that's fixed.
        console.error(reason)
      })
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
