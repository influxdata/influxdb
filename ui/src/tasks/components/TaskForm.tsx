// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  Radio,
  Input,
  Button,
  ComponentSpacer,
} from '@influxdata/clockface'
import {Grid} from 'src/clockface'
import TaskScheduleFormField from 'src/tasks/components/TaskScheduleFormField'
import TaskOptionsBucketDropdown from 'src/tasks/components/TasksOptionsBucketDropdown'
import GetResources, {
  ResourceTypes,
} from 'src/configuration/components/GetResources'

// Types
import {
  Stack,
  Columns,
  Alignment,
  ButtonType,
  ButtonShape,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {TaskOptions, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'
import {RemoteDataState} from 'src/types'

interface Props {
  taskOptions: TaskOptions
  isInOverlay: boolean
  canSubmit: boolean
  onSubmit: () => void
  dismiss: () => void
  onChangeScheduleType: (schedule: TaskSchedule) => void
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
  onChangeToOrgName: (orgName: string) => void
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
    onChangeToOrgName: () => {},
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
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column>
              <Form.Element label="Schedule Task">
                <ComponentSpacer
                  align={Alignment.Left}
                  stackChildren={Stack.Rows}
                >
                  <Radio shape={ButtonShape.StretchToFit}>
                    <Radio.Button
                      id="every"
                      active={taskScheduleType === TaskSchedule.interval}
                      value={TaskSchedule.interval}
                      titleText="Run task at regular intervals"
                      onClick={this.handleChangeScheduleType}
                    >
                      Every
                    </Radio.Button>
                    <Radio.Button
                      id="cron"
                      active={taskScheduleType === TaskSchedule.cron}
                      value={TaskSchedule.cron}
                      titleText="Use cron syntax for more control over scheduling"
                      onClick={this.handleChangeScheduleType}
                    >
                      Cron
                    </Radio.Button>
                  </Radio>
                  {this.cronHelper}
                </ComponentSpacer>
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
              <Grid.Column widthXS={Columns.Six}>
                <Form.Element label="Output Bucket">
                  <GetResources resource={ResourceTypes.Buckets}>
                    <TaskOptionsBucketDropdown
                      selectedBucketName={toBucketName}
                      onChangeBucketName={onChangeToBucketName}
                      loading={RemoteDataState.Done}
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
