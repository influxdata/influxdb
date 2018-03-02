import reducer, {initialState} from 'src/shared/reducers/notifications'

import {
  publishNotification,
  dismissNotification,
  dismissAllNotifications,
} from 'src/shared/actions/notifications'

const notificationID = '000'

const exampleNotification = {
  id: notificationID,
  type: 'success',
  message: 'Hell yeah you are a real notification!',
  duration: 5000,
  icon: 'zap',
}

describe('fsd', () => {
  it('should publish a notification', () => {
    const actual = reducer(
      initialState,
      publishNotification(exampleNotification)
    )
    const expected = [...initialState, exampleNotification]

    expect(actual).to.equal(expected)
  })

  it('should dismiss a notification', () => {
    const actual = reducer(initialState, dismissNotification(notificationID))
    const expected = initialState.filter(n => n.id !== notificationID)

    expect(actual).to.equal(expected)
  })

  it('should dismiss all notifications', () => {
    const actual = reducer(initialState, dismissAllNotifications())
    const expected = initialState

    expect(actual).to.equal(expected)
  })
})
