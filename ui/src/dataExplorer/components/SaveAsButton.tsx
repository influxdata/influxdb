// Libraries
import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {IconFont, Button, ComponentColor} from '@influxdata/clockface'

class SaveAsButton extends PureComponent<RouteComponentProps, {}> {
  public render() {
    return (
      <>
        <Button
          icon={IconFont.Export}
          text="Save As"
          onClick={this.handleShowOverlay}
          color={ComponentColor.Primary}
          titleText="Save your query as a Dashboard Cell or a Task"
          testID="save-query-as"
        />
      </>
    )
  }

  private handleShowOverlay = () => {
    const {
      location: {pathname},
    } = this.props

    this.props.history.push(`${pathname}/save`)
  }
}

export default withRouter(SaveAsButton)
