// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  SelectGroup,
  Input,
  Button,
  FlexBox,
  Grid,
} from '@influxdata/clockface'
import TaskScheduleFormField from 'src/tasks/components/TaskScheduleFormField'
import TaskOptionsBucketDropdown from 'src/tasks/components/TasksOptionsBucketDropdown'
import GetResources from 'src/resources/components/GetResources'

// Types
import {
  Columns,
  ButtonType,
  ButtonShape,
  ComponentColor,
  ComponentStatus,
  FlexDirection,
  AlignItems,
  ComponentSize,
} from '@influxdata/clockface'
import {ResourceType, TaskOptions, TaskSchedule} from 'src/types'

interface Props {
  taskOptions: TaskOptions
  isInOverlay: boolean
  canSubmit: boolean
  onSubmit: () => void
  dismiss: () => void
  onChangeScheduleType: (schedule: TaskSchedule) => void
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
  onChangeToBucketName: (bucketName: string) => void
}

interface State {
  schedule: TaskSchedule
}

export default class TaskForm extends PureComponent<Props, State> {
  public static defaultProps = {
    isInOverlay: false,
    canSubmit: true,
    onSubmit: () => {},
    dismiss: () => {},
    onChangeToBucketName: () => {},
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      schedule: props.taskOptions.taskScheduleType,
    }
  }

  public render() {
    const {
      onChangeInput,
      onChangeToBucketName,
      taskOptions: {
        name,
        taskScheduleType,
        interval,
        offset,
        cron,
        toBucketName,
      },
      isInOverlay,
    } = this.props

    return (
      <Form>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Name">
                <Input
                  name="name"
                  placeholder="Name your task"
                  onChange={onChangeInput}
                  value={name}
                  testID="task-form-name"
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column>
              <Form.Element label="Schedule Task">
                <FlexBox
                  direction={FlexDirection.Column}
                  alignItems={AlignItems.FlexStart}
                  margin={ComponentSize.Small}
                >
                  <SelectGroup shape={ButtonShape.StretchToFit}>
                    <SelectGroup.Option
                      name="task-schedule"
                      id="every"
                      active={taskScheduleType === TaskSchedule.interval}
                      value={TaskSchedule.interval}
                      titleText="Run task at regular intervals"
                      onClick={this.handleChangeScheduleType}
                      testID="task-card-every-btn"
                    >
                      Every
                    </SelectGroup.Option>
                    <SelectGroup.Option
                      name="task-schedule"
                      id="cron"
                      active={taskScheduleType === TaskSchedule.cron}
                      value={TaskSchedule.cron}
                      titleText="Use cron syntax for more control over scheduling"
                      onClick={this.handleChangeScheduleType}
                      testID="task-card-cron-btn"
                    >
                      Cron
                    </SelectGroup.Option>
                  </SelectGroup>
                  {this.cronHelper}
                </FlexBox>
              </Form.Element>
            </Grid.Column>
            <TaskScheduleFormField
              onChangeInput={onChangeInput}
              schedule={taskScheduleType}
              interval={interval}
              offset={offset}
              cron={cron}
            />
            {isInOverlay && (
              <Grid.Column widthXS={Columns.Twelve}>
                <Form.Element label="Output Bucket">
                  <GetResources resources={[ResourceType.Buckets]}>
                    <TaskOptionsBucketDropdown
                      selectedBucketName={toBucketName}
                      onChangeBucketName={onChangeToBucketName}
                    />
                  </GetResources>
                </Form.Element>
              </Grid.Column>
            )}
            {isInOverlay && this.buttons}
          </Grid.Row>
        </Grid>
      </Form>
    )
  }

  private get buttons(): JSX.Element {
    const {onSubmit, canSubmit, dismiss} = this.props
    return (
      <Grid.Column widthXS={Columns.Twelve}>
        <Form.Footer>
          <Button
            text="Cancel"
            onClick={dismiss}
            titleText="Cancel save"
            type={ButtonType.Button}
          />
          <Button
            text="Save as Task"
            color={ComponentColor.Success}
            type={ButtonType.Submit}
            onClick={onSubmit}
            status={
              canSubmit ? ComponentStatus.Default : ComponentStatus.Disabled
            }
            testID="task-form-save"
          />
        </Form.Footer>
      </Grid.Column>
    )
  }

  private get cronHelper(): JSX.Element {
    const {taskOptions} = this.props

    if (taskOptions.taskScheduleType === TaskSchedule.cron) {
      return (
        <Form.Box>
          <p className="time-format--helper">
            For more information on cron syntax,{' '}
            <a href="https://crontab.guru/" target="_blank">
              click here
            </a>
          </p>
        </Form.Box>
      )
    }
  }

  private handleChangeScheduleType = (schedule: TaskSchedule): void => {
    this.props.onChangeScheduleType(schedule)
  }
}
