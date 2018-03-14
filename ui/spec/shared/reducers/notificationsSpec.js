import reducer, {initialState} from 'shared/reducers/notifications'

import {
  publishNotification,
  dismissNotification,
} from 'shared/actions/notifications'

import {FIVE_SECONDS} from 'shared/constants/index'

const notificationID = '000'

const exampleNotification = {
  id: notificationID,
  type: 'success',
  message: 'Hell yeah you are a real notification!',
  duration: FIVE_SECONDS,
  icon: 'zap',
}

const exampleNotifications = [exampleNotification]

describe('Shared.Reducers.notifications', () => {
  it('should publish a notification', () => {
    const actual = reducer(
      initialState,
      publishNotification(exampleNotification)
    )
    const expected = [...initialState, exampleNotification]

    expect(actual).to.equal(expected)
  })

  it('should dismiss a notification', () => {
    const actual = reducer(
      exampleNotifications,
      dismissNotification(notificationID)
    )

    expect(actual.length).to.be(0)
  })
})
