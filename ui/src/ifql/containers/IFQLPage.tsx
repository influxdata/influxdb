import React, {PureComponent} from 'react'
import TimeMachine from 'src/ifql/components/TimeMachine'
import {connect} from 'react-redux'

interface Links {
  self: string
  suggestions: string
}

interface Props {
  links: Links
}

export class IFQLPage extends PureComponent<Props, {}> {
  public render() {
    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Time Machine</h1>
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <TimeMachine />
          </div>
        </div>
      </div>
    )
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.ifql}
}

export default connect(mapStateToProps, null)(IFQLPage)
