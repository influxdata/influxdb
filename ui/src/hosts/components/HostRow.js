import React, {PropTypes, Component} from 'react'
import shallowCompare from 'react-addons-shallow-compare'
import {Link} from 'react-router'
import classnames from 'classnames'

import {HOSTS_TABLE} from 'src/hosts/constants/tableSizing'

class HostRow extends Component {
  constructor(props) {
    super(props)
  }

  shouldComponentUpdate(nextProps) {
    return shallowCompare(this, nextProps)
  }

  render() {
    const {host, source} = this.props
    const {name, cpu, load, apps = []} = host
    const {colName, colStatus, colCPU, colLoad} = HOSTS_TABLE

    return (
      <tr>
        <td style={{width: colName}}>
          <Link to={`/sources/${source.id}/hosts/${name}`}>
            {name}
          </Link>
        </td>
        <td style={{width: colStatus}}>
          <div
            className={classnames(
              'table-dot',
              Math.max(host.deltaUptime || 0, host.winDeltaUptime || 0) > 0
                ? 'dot-success'
                : 'dot-critical'
            )}
          />
        </td>
        <td style={{width: colCPU}} className="monotype">
          {isNaN(cpu) ? 'N/A' : `${cpu.toFixed(2)}%`}
        </td>
        <td style={{width: colLoad}} className="monotype">
          {isNaN(load) ? 'N/A' : `${load.toFixed(2)}`}
        </td>
        <td>
          {apps.map((app, index) => {
            return (
              <span key={app}>
                <Link
                  style={{marginLeft: '2px'}}
                  to={{
                    pathname: `/sources/${source.id}/hosts/${name}`,
                    query: {app},
                  }}
                >
                  {app}
                </Link>
                {index === apps.length - 1 ? null : ', '}
              </span>
            )
          })}
        </td>
      </tr>
    )
  }
}

HostRow.propTypes = {
  source: PropTypes.shape({
    id: PropTypes.string.isRequired,
    name: PropTypes.string.isRequired,
  }).isRequired,
  host: PropTypes.shape({
    name: PropTypes.string,
    cpu: PropTypes.number,
    load: PropTypes.number,
    deltaUptime: PropTypes.number.required,
    apps: PropTypes.arrayOf(PropTypes.string.isRequired),
  }),
}

export default HostRow
