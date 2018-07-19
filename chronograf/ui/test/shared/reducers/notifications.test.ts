import {initialState, notifications} from 'src/shared/reducers/notifications'

import {notify, dismissNotification} from 'src/shared/actions/notifications'

import {FIVE_SECONDS} from 'src/shared/constants/index'

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
    const [actual] = notifications(initialState, notify(exampleNotification))

    const [expected] = [exampleNotification, ...initialState]

    expect(actual.type).toEqual(expected.type)
    expect(actual.icon).toEqual(expected.icon)
    expect(actual.message).toEqual(expected.message)
    expect(actual.duration).toEqual(expected.duration)
  })

  describe('adding more than one notification', () => {
    it('should put the new notification at the beggining of the list', () => {
      const newNotification = {
        type: 'error',
        message: 'new notification',
        duration: FIVE_SECONDS,
        icon: 'zap',
      }

      const actual = notifications(
        exampleNotifications,
        notify(newNotification)
      )

      expect(actual.length).toBe(2)
      expect(actual[0].message).toEqual(newNotification.message)
    })
  })

  it('should dismiss a notification', () => {
    const actual = notifications(
      exampleNotifications,
      dismissNotification(notificationID)
    )

    expect(actual.length).toBe(0)
  })
})
