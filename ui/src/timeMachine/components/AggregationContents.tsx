// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  Input,
  InputLabel,
  FlexBox,
  FlexDirection,
  ComponentSize,
  SelectGroup,
  InputType,
} from '@influxdata/clockface'
import SelectorList from 'src/timeMachine/components/SelectorList'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import DurationInput from 'src/shared/components/DurationInput'

// Utils
import {millisecondsToDuration} from 'src/shared/utils/duration'

// Constants
import {
  AGG_WINDOW_AUTO,
  AGG_WINDOW_NONE,
  DURATIONS,
} from 'src/timeMachine/constants/queryBuilder'

// Types

interface Props {
  isAutoWindowPeriod: boolean
  isAutoFunction: boolean
  isFillValues: boolean
  windowPeriod: string
  selectedFunctions: Array<string>
  functionList: Array<string>
  isInCheckOverlay: boolean
  onSelectFunction: (name: string) => void
}

class AggregationContents extends PureComponent<Props> {
  public render() {
    const {
      isAutoWindowPeriod,
      isAutoFunction,
      isFillValues,
      windowPeriod,
      selectedFunctions,
      functionList,
      isInCheckOverlay,
      onSelectFunction,
    } = this.props
    return (
      <BuilderCard
        className="aggregation-selector"
        testID="aggregation-selector"
      >
        <BuilderCard.Menu>
          <FlexBox
            direction={FlexDirection.Column}
            margin={ComponentSize.Small}
          >
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
              testID="component-spacer"
            >
              Window Period
            </FlexBox>
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
              testID="component-spacer"
            >
              <SelectGroup>
                <SelectGroup.Option
                  name="custom"
                  id="custom-window-period"
                  active={!isAutoWindowPeriod}
                  value="Custom"
                  onClick={() => {}}
                  titleText="Custom"
                >
                  Custom
                </SelectGroup.Option>
                <SelectGroup.Option
                  name="auto"
                  id="auto-window-period"
                  active={isAutoWindowPeriod}
                  value="Auto"
                  onClick={() => {}}
                  titleText="Auto"
                >
                  Auto
                </SelectGroup.Option>
              </SelectGroup>
            </FlexBox>
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
              testID="component-spacer"
            >
              <FlexBox.Child>
                <DurationInput
                  onSubmit={() => {}}
                  value={windowPeriod}
                  suggestions={this.durations}
                  submitInvalid={false}
                  validFunction={this.windowInputValid}
                />
              </FlexBox.Child>
            </FlexBox>
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
              testID="component-spacer"
            >
              <FlexBox.Child basis={20} grow={0}>
                <Input
                  type={InputType.Checkbox}
                  size={ComponentSize.ExtraSmall}
                  checked={isFillValues}
                  onChange={() => {}}
                />
              </FlexBox.Child>
              <FlexBox.Child grow={1}>
                <InputLabel>Fill missing values</InputLabel>
              </FlexBox.Child>
            </FlexBox>
          </FlexBox>
          <FlexBox
            direction={FlexDirection.Column}
            margin={ComponentSize.Small}
          >
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
              testID="component-spacer"
            >
              Aggregate Function
            </FlexBox>
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
              testID="component-spacer"
            >
              <SelectGroup>
                <SelectGroup.Option
                  name="custom"
                  id="custom window period"
                  active={!isAutoFunction}
                  value="Custom"
                  onClick={() => {}}
                  titleText="Custom"
                >
                  Custom
                </SelectGroup.Option>
                <SelectGroup.Option
                  name="template-type"
                  id="user-templates"
                  active={isAutoFunction}
                  value="user-templates"
                  onClick={() => {}}
                  titleText="User Templates"
                >
                  Auto
                </SelectGroup.Option>
              </SelectGroup>
            </FlexBox>
          </FlexBox>
        </BuilderCard.Menu>
        <SelectorList
          items={functionList}
          selectedItems={selectedFunctions}
          onSelectItem={onSelectFunction}
          multiSelect={!isInCheckOverlay && !isAutoFunction}
        />
      </BuilderCard>
    )
  }

  private get autoLabel(): string {
    const {windowPeriod} = this.props
    return windowPeriod
      ? `${AGG_WINDOW_AUTO} (${millisecondsToDuration(10)})`
      : AGG_WINDOW_AUTO
  }

  private get durations(): string[] {
    return this.props.isInCheckOverlay
      ? DURATIONS
      : [this.autoLabel, AGG_WINDOW_NONE, ...DURATIONS]
  }

  private windowInputValid = (input: string): boolean =>
    input == 'none' || input == this.autoLabel
}

export default AggregationContents
