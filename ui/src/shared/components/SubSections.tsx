import React, {Component, ReactNode} from 'react'
import uuid from 'uuid'
import {withRouter, InjectedRouter} from 'react-router'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Section {
  url: string
  name: string
  component: ReactNode
  enabled: boolean
}

interface Props {
  sections: Section[]
  activeSection: string
  sourceID: string
  router: InjectedRouter
  parentUrl: string
}

@ErrorHandling
@withRouter
class SubSections extends Component<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {sections} = this.props

    return (
      <div className="row subsection">
        <div className="col-md-2 subsection--nav">
          <div className="subsection--tabs">
            {sections.map(
              section =>
                section.enabled && (
                  <div
                    key={uuid.v4()}
                    className={this.getTabClass(section.url)}
                    onClick={this.handleTabClick(section.url)}
                  >
                    {section.name}
                  </div>
                )
            )}
          </div>
        </div>
        <div className="col-md-10 subsection--content">
          {this.activeSection}
        </div>
      </div>
    )
  }

  private get activeSection(): ReactNode {
    const {sections, activeSection} = this.props
    const {component} = sections.find(section => section.url === activeSection)
    return component
  }

  public getTabClass(sectionName: string) {
    const {activeSection} = this.props
    return `subsection--tab ${sectionName === activeSection ? 'active' : ''}`
  }

  public handleTabClick = url => () => {
    const {router, sourceID, parentUrl} = this.props
    router.push(`/sources/${sourceID}/${parentUrl}/${url}`)
  }
}

export default SubSections
