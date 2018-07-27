import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import InfluxTableHead from 'src/sources/components/InfluxTableHead'
import InfluxTableHeader from 'src/sources/components/InfluxTableHeader'
import InfluxTableRow from 'src/sources/components/InfluxTableRow'

import {Source, Me, Service} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  me: Me
  source: Source
  sources: Source[]
  services: Service[]
  isUsingAuth: boolean
  onDeleteSource: (source: Source) => void
  setActiveFlux: (source: Source, service: Service) => void
  deleteFlux: (fluxService: Service) => void
}

@ErrorHandling
class InfluxTable extends PureComponent<Props> {
  public render() {
    const {
      source,
      sources,
      setActiveFlux,
      onDeleteSource,
      deleteFlux,
      isUsingAuth,
      me,
    } = this.props

    return (
      <div className="row">
        <div className="col-md-12">
          <div className="panel">
            <InfluxTableHeader
              me={me}
              source={source}
              isUsingAuth={isUsingAuth}
            />
            <div className="panel-body">
              <table className="table v-center margin-bottom-zero table-highlight">
                <InfluxTableHead />
                <tbody>
                  {sources.map(s => {
                    return (
                      <InfluxTableRow
                        key={s.id}
                        source={s}
                        services={this.getServicesForSource(s.id)}
                        currentSource={source}
                        onDeleteSource={onDeleteSource}
                        setActiveFlux={setActiveFlux}
                        deleteFlux={deleteFlux}
                      />
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    )
  }

  private getServicesForSource(sourceID: string) {
    return this.props.services.filter(s => {
      return s.sourceID === sourceID
    })
  }
}

const mapStateToProps = ({auth: {isUsingAuth, me}}) => ({isUsingAuth, me})

export default connect(mapStateToProps)(InfluxTable)
