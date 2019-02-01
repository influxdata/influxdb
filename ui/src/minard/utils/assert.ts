export const assert = (message: string, condition: boolean) => {
  if (!condition) {
    throw new Error(message)
  }
}
