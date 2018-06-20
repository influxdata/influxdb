enum AlertType {
  'info',
}

export type ErrorThrownActionCreator = (
  error: Error,
  altText?: string,
  alertType?: AlertType
) => ErrorThrownAction

interface ErrorThrownAction {
  type: 'ERROR_THROWN'
  error: Error
  altText?: string
  alertType?: AlertType
}
export const errorThrown = (
  error: Error,
  altText?: string,
  alertType?: AlertType
): ErrorThrownAction => ({
  type: 'ERROR_THROWN',
  error,
  altText,
  alertType,
})
