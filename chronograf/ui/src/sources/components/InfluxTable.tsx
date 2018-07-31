import React, {PureComponent} from 'react'

import InfluxTableHead from 'src/sources/components/InfluxTableHead'
import InfluxTableHeader from 'src/sources/components/InfluxTableHeader'
import InfluxTableRow from 'src/sources/components/InfluxTableRow'

import {Source} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
  sources: Source[]
  onDeleteSource: (source: Source) => void
  setActiveFlux: (source: Source, service: Service) => void
  deleteFlux: (fluxService: Service) => void
}

@ErrorHandling
class InfluxTable extends PureComponent<Props> {
  public render() {
    const {source, sources, onDeleteSource} = this.props

    return (
      <div className="row">
        <div className="col-md-12">
          <div className="panel">
            <InfluxTableHeader source={source} />
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

export default InfluxTable
