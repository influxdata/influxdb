// Libraries
import React, {PureComponent} from 'react'

// Components
import SaveAsCellForm from 'src/dataExplorer/components/SaveAsCellForm'
import SaveAsTaskForm from 'src/dataExplorer/components/SaveAsTaskForm'
import {IconFont, Button, ComponentColor} from '@influxdata/clockface'
import {
  Radio,
  OverlayTechnology,
  OverlayBody,
  OverlayHeading,
  OverlayContainer,
} from 'src/clockface'

// Styles
import 'src/dataExplorer/components/SaveAsButton.scss'

enum SaveAsOption {
  Dashboard = 'dashboard',
  Task = 'task',
}

interface Props {}

interface State {
  isOverlayVisible: boolean
  saveAsOption: SaveAsOption
}

class SaveAsButton extends PureComponent<Props, State> {
  public state: State = {
    isOverlayVisible: false,
    saveAsOption: SaveAsOption.Dashboard,
  }

  public render() {
    const {isOverlayVisible, saveAsOption} = this.state

    return (
      <>
        <Button
          icon={IconFont.Export}
          text="Save As"
          onClick={this.handleShowOverlay}
          color={ComponentColor.Primary}
          titleText="Save your query as a Dashboard Cell or a Task"
        />
        <OverlayTechnology visible={isOverlayVisible}>
          <OverlayContainer maxWidth={600}>
            <OverlayHeading
              title="Save As"
              onDismiss={this.handleHideOverlay}
            />
            <OverlayBody>
              <div className="save-as--options">
                <Radio>
                  <Radio.Button
                    active={saveAsOption === SaveAsOption.Dashboard}
                    value={SaveAsOption.Dashboard}
                    onClick={this.handleSetSaveAsOption}
                    data-test="cell-radio-button"
                  >
                    Dashboard Cell
                  </Radio.Button>
                  <Radio.Button
                    active={saveAsOption === SaveAsOption.Task}
                    value={SaveAsOption.Task}
                    onClick={this.handleSetSaveAsOption}
                    data-test="task-radio-button"
                  >
                    Task
                  </Radio.Button>
                </Radio>
              </div>
              {this.saveAsForm}
            </OverlayBody>
          </OverlayContainer>
        </OverlayTechnology>
      </>
    )
  }

  private get saveAsForm(): JSX.Element {
    const {saveAsOption} = this.state

    if (saveAsOption === SaveAsOption.Dashboard) {
      return <SaveAsCellForm dismiss={this.handleHideOverlay} />
    } else if (saveAsOption === SaveAsOption.Task) {
      return <SaveAsTaskForm dismiss={this.handleHideOverlay} />
    }
  }

  private handleShowOverlay = () => {
    this.setState({isOverlayVisible: true})
  }

  private handleHideOverlay = () => {
    this.setState({isOverlayVisible: false})
  }

  private handleSetSaveAsOption = (saveAsOption: SaveAsOption) => {
    this.setState({saveAsOption})
  }
}

export default SaveAsButton
