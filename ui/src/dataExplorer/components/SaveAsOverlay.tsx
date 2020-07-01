import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import SaveAsCellForm from 'src/dataExplorer/components/SaveAsCellForm'
import SaveAsTaskForm from 'src/dataExplorer/components/SaveAsTaskForm'
import SaveAsVariable from 'src/dataExplorer/components/SaveAsVariable'
import {
  Overlay,
  Tabs,
  Alignment,
  ComponentSize,
  Orientation,
} from '@influxdata/clockface'

enum SaveAsOption {
  Dashboard = 'dashboard',
  Task = 'task',
  Variable = 'variable',
}

interface State {
  saveAsOption: SaveAsOption
}

class SaveAsOverlay extends PureComponent<RouteComponentProps, State> {
  public state: State = {
    saveAsOption: SaveAsOption.Dashboard,
  }

  render() {
    const {saveAsOption} = this.state

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={600}>
          <Overlay.Header
            title="Save As"
            onDismiss={this.handleHideOverlay}
            testID="save-as-overlay--header"
          />
          <Overlay.Body>
            <Tabs.Container orientation={Orientation.Horizontal}>
              <Tabs alignment={Alignment.Center} size={ComponentSize.Medium}>
                <Tabs.Tab
                  id={SaveAsOption.Dashboard}
                  text="Dashboard Cell"
                  testID="cell-radio-button"
                  onClick={this.handleSetSaveAsOption}
                  active={saveAsOption === SaveAsOption.Dashboard}
                />
                <Tabs.Tab
                  id={SaveAsOption.Task}
                  text="Task"
                  testID="task--radio-button"
                  onClick={this.handleSetSaveAsOption}
                  active={saveAsOption === SaveAsOption.Task}
                />
                <Tabs.Tab
                  id={SaveAsOption.Variable}
                  text="Variable"
                  testID="variable-radio-button"
                  onClick={this.handleSetSaveAsOption}
                  active={saveAsOption === SaveAsOption.Variable}
                />
              </Tabs>
              <Tabs.TabContents>{this.saveAsForm}</Tabs.TabContents>
            </Tabs.Container>
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get saveAsForm(): JSX.Element {
    const {saveAsOption} = this.state

    if (saveAsOption === SaveAsOption.Dashboard) {
      return <SaveAsCellForm dismiss={this.handleHideOverlay} />
    } else if (saveAsOption === SaveAsOption.Task) {
      return <SaveAsTaskForm dismiss={this.handleHideOverlay} />
    } else if (saveAsOption === SaveAsOption.Variable) {
      return <SaveAsVariable onHideOverlay={this.handleHideOverlay} />
    }
  }

  private handleHideOverlay = () => {
    this.props.history.goBack()
  }

  private handleSetSaveAsOption = (saveAsOption: SaveAsOption) => {
    this.setState({saveAsOption})
  }
}

export default withRouter(SaveAsOverlay)
